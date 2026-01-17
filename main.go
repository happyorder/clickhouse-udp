package main

import (
	"context"
	"encoding/binary"

	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"

	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ParsedPacket struct {
	tableName string
	columns   []string
	values    []interface{}
}

type ColValue struct {
	column string
	value  interface{}
}

type DbTableColumnMap = map[string][]ColumnInfo

type CurrentDbSchema struct {
	Schema *DbTableColumnMap
}

type ColumnInfo struct {
	Table    string
	ColType  string
	Name     string
	Position uint64
}

func (p *ParsedPacket) PacketKey() string {
	return fmt.Sprintf("%s:%s", p.tableName, strings.Join(p.columns, ","))
}

func main() {
	exitCtx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	var dbConn, err = clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		//Debug:           true,
		DialTimeout:     time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	})

	if err != nil {
		log.Panicln(err)
		return
	}
	defer dbConn.Close()

	schema, err := fetchClickhouseSchema(dbConn)
	if err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Panicf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			log.Panicln(err)
		}
		return
	}

	currentSchema := CurrentDbSchema{
		Schema: &schema,
	}
	queuedUnparsed := make(chan *ParsedPacket, 5000)

	go func() {
		startUdpServer(exitCtx, queuedUnparsed)
	}()

	go func() {
		startTcpServer(exitCtx, queuedUnparsed)
	}()

	go func() {
		updateDbSchemaThread(exitCtx, dbConn, &currentSchema)
	}()

	go func() {
		<-exitCtx.Done()
		log.Println("Exit signal received, flushing batches...")
	}()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		createLogsSaveThread(exitCtx, dbConn, &currentSchema, queuedUnparsed, 30*time.Second, 2000)
	}()

	wg.Wait()
}

func updateDbSchemaThread(exitCtx context.Context, dbConn driver.Conn, currentSchema *CurrentDbSchema) {
	for {
		expire := time.After(time.Minute * 10)

		select {
		case <-exitCtx.Done():
			return

		case <-expire:
			schema, err := fetchClickhouseSchema(dbConn)
			if err != nil {
				if exception, ok := err.(*clickhouse.Exception); ok {
					log.Printf("fetchClickhouseSchema error [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
				} else {
					log.Println("fetchClickhouseSchema error", err)
				}
			} else {
				currentSchema.Schema = &schema
			}
		}
	}
}

type QueuedPacketList struct {
	Packets          []*ParsedPacket
	OldestPacketTime time.Time
}

func createLogsSaveThread(interruptCtx context.Context, clickh clickhouse.Conn, schema *CurrentDbSchema, values <-chan *ParsedPacket, maxTimeout time.Duration, maxItems int) {
	isExiting := false
	var queueByTable = make(map[string]*QueuedPacketList)

	for {
		expire := time.After(maxTimeout)

		for {
			select {
			case value, ok := <-values:
				if !ok {
					isExiting = true
					goto sendBatch
				}
				tableKey := value.PacketKey()
				tableList := queueByTable[tableKey]

				if tableList == nil {
					tableList = &QueuedPacketList{
						OldestPacketTime: time.Now(),
						Packets:          make([]*ParsedPacket, 0),
					}
					queueByTable[tableKey] = tableList
				}

				tableList.Packets = append(tableList.Packets, value)

				if len(tableList.Packets) >= maxItems {
					goto sendBatch
				}

			case <-expire:
				goto sendBatch

			case <-interruptCtx.Done():
				isExiting = true
				goto sendBatch
			}
		}

	sendBatch:
		for tableKey, packetList := range queueByTable {
			if len(packetList.Packets) == 0 {
				continue
			}

			age := time.Since(packetList.OldestPacketTime)
			shouldSend := isExiting || age >= maxTimeout || len(packetList.Packets) >= maxItems

			if !shouldSend {
				continue
			}

			delete(queueByTable, tableKey)

			fmt.Println("Saving batch of", len(packetList.Packets), "to table", packetList.Packets[0].tableName, ",", int(age.Seconds()), "seconds old")

			for i := 0; i < 5; i++ {
				shouldRetry := insertIntoDb(clickh, packetList.Packets, *schema.Schema)
				if !shouldRetry {
					break
				}
				fmt.Println("Retrying insert in 5 seconds")
				time.Sleep(time.Second * 5)
			}
		}

		if isExiting {
			return
		}
	}
}

func startUdpServer(exitCtx context.Context, packetQueue chan<- *ParsedPacket) {
	addr := net.UDPAddr{
		Port: 9030,
		IP:   net.ParseIP("0.0.0.0"),
	}

	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	log.Println("Listening for UDP on", conn.LocalAddr())

	go func() {
		<-exitCtx.Done()
		conn.Close()
	}()

	var buf [1024 * 8]byte
	for {
		select {
		case <-exitCtx.Done():
			return
		default:
		}

		rlen, _, err := conn.ReadFromUDP(buf[:])

		if err != nil {
			select {
			case <-exitCtx.Done():
				return
			default:
				fmt.Println("read error", err)
				continue
			}
		}

		packetQueue <- parsePacket(buf[:rlen])
	}
}

func startTcpServer(exitCtx context.Context, packetQueue chan<- *ParsedPacket) {
	addr := net.TCPAddr{
		Port: 9030,
		IP:   net.ParseIP("0.0.0.0"),
	}
	listener, err := net.ListenTCP("tcp", &addr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	log.Println("Listening for TCP on", listener.Addr().String())

	go func() {
		<-exitCtx.Done()
		listener.Close()
	}()

	for {
		select {
		case <-exitCtx.Done():
			return
		default:
		}

		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-exitCtx.Done():
				return
			default:
				fmt.Println("Error accepting:", err)
				continue
			}
		}

		fmt.Println("New tcp client")

		go handleTcpConn(conn, packetQueue)
	}
}

func handleTcpConn(conn net.Conn, packetQueue chan<- *ParsedPacket) {
	defer conn.Close()
	const DEADLINE = 30

	headerBuff := make([]byte, 2)
	OK := []byte("K")
	ERROR_RESP := []byte("E")

	for {
		conn.SetDeadline(time.Now().Add(DEADLINE * time.Second))

		headerLen, err := io.ReadFull(conn, headerBuff)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// fmt.Println("Tcp client timed out")
			} else if err == io.EOF {
				// This is normal, we just want to exit
			} else {
				fmt.Println("Error reading tcp header:", err)
			}
			return
		}

		if headerLen != 2 {
			fmt.Println("Tcp header was only", headerLen, "bytes")
			return
		}

		payloadSize := int(binary.BigEndian.Uint16(headerBuff))

		if payloadSize > 1024*32 {
			fmt.Println("Tcp payload too big", payloadSize)
			return
		}

		// fmt.Println("Got tcp header with size", payloadSize)

		payloadBuff := make([]byte, payloadSize)
		bytesRead, err := io.ReadFull(conn, payloadBuff)

		if err != nil {
			fmt.Println("Tcp read error", err)
			return
		}

		// fmt.Println("Got tcp payload with size", bytesRead)

		if bytesRead == payloadSize {
			parsed := parsePacket(payloadBuff[:payloadSize])
			packetQueue <- parsed

			if parsed != nil {
				_, err = conn.Write(OK)
			} else {
				_, err = conn.Write(ERROR_RESP)
			}

			if err != nil {
				fmt.Println("Tcp write error", err)
				return
			}
		}
	}
}

func fetchClickhouseSchema(dbConn driver.Conn) (DbTableColumnMap, error) {
	fmt.Println("Fetching column info...")

	rows, err := dbConn.Query(context.Background(), `
SELECT table, position, type, name
FROM system.columns
WHERE database = 'default'`)

	result := make(DbTableColumnMap)

	if err != nil {
		return result, err
	}

	for rows.Next() {
		var (
			table   string
			pos     uint64
			colType string
			name    string
		)
		if err := rows.Scan(&table, &pos, &colType, &name); err != nil {
			return result, err
		}

		result[table] = append(result[table], ColumnInfo{
			Table:    table,
			Position: pos,
			ColType:  normalizeClickHouseType(colType),
			Name:     name,
		})
	}

	fmt.Println("...Fetched columns for", len(result), "tables")

	return result, err
}

var re = regexp.MustCompile(`\(\d+\)`)
var re2 = regexp.MustCompile(`LowCardinality\(([^\)]+)\)`)
var re3 = regexp.MustCompile(`Nullable\(([^\)]+)\)`)

func normalizeClickHouseType(colType string) string {
	// Seen clickhouse datatypes:
	// UInt8
	// UInt16
	// UInt32
	// UInt64
	// String
	// Nullable(Float32)
	// Nullable(Float64)
	// Map(String, String)
	// Map(String, Float32)
	// Map(String, Float64)
	// Array(UInt8)
	// Array(String)
	// Array(LowCardinality(String))
	// Map(LowCardinality(String), String)
	// Map(LowCardinality(String), Float32)
	// Map(LowCardinality(String), Float64)
	// LowCardinality(String)
	// Int32
	// Int64
	// Float32
	// Float64
	// DateTime
	// DateTime64(3)
	// DateTime64(9)

	normalized := strings.TrimSpace(strings.ReplaceAll(colType, " ", ""))
	normalized = re.ReplaceAllString(normalized, "")
	normalized = re2.ReplaceAllString(normalized, "$1")
	normalized = re3.ReplaceAllString(normalized, "$1")

	return normalized
}

func insertIntoDb(dbConn driver.Conn, rows []*ParsedPacket, columnInfo DbTableColumnMap) bool {
	tableName := rows[0].tableName
	packetColumnNames := rows[0].columns
	dbColumns := columnInfo[tableName]

	if dbColumns == nil {
		fmt.Println("No column info found for table", tableName)
		return false
	}

	tx, err := dbConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s", tableName))
	if err != nil {
		fmt.Println("Could not start transaction", err)
		return true
	}

	if tx == nil {
		fmt.Println("Could not start transaction")
		return true
	}

	defer tx.Abort()

	for _, dbCol := range dbColumns {
		packetColumnIndex := -1
		for i, name := range packetColumnNames {
			if name == dbCol.Name {
				packetColumnIndex = i
				break
			}
		}

		col := tx.Column(int(dbCol.Position) - 1)
		if col == nil {
			fmt.Println("Could not get batch column for", tableName, dbCol.Name)
			return false
		}

		if err := appendColumnData(col, dbCol.ColType, rows, packetColumnIndex, tableName, dbCol.Name); err != nil {
			fmt.Println("Add column error", tableName, dbCol.Name, err)
			return false
		}
	}

	if err := tx.Send(); err != nil {
		fmt.Println("Commit error", tableName, err)
		return true
	}

	return false
}

func appendColumnData(col driver.BatchColumn, colType string, rows []*ParsedPacket, colIndex int, tableName, colName string) error {
	switch colType {
	case "String":
		items := make([]string, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToString(row.values[colIndex])
			}
		}
		return col.Append(items)

	case "Float32":
		items := make([]float32, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToFloat32(row.values[colIndex], tableName, colName)
			}
		}
		return col.Append(items)

	case "Float64":
		items := make([]float64, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToFloat64(row.values[colIndex], tableName, colName)
			}
		}
		return col.Append(items)

	case "Int8":
		items := make([]int8, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToInt8(row.values[colIndex], tableName, colName)
			}
		}
		return col.Append(items)

	case "Int16":
		items := make([]int16, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToInt16(row.values[colIndex], tableName, colName)
			}
		}
		return col.Append(items)

	case "Int32":
		items := make([]int32, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToInt32(row.values[colIndex], tableName, colName)
			}
		}
		return col.Append(items)

	case "Int64":
		items := make([]int64, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToInt64(row.values[colIndex], tableName, colName)
			}
		}
		return col.Append(items)

	case "UInt8":
		items := make([]uint8, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToUInt8(row.values[colIndex], tableName, colName)
			}
		}
		return col.Append(items)

	case "UInt16":
		items := make([]uint16, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToUInt16(row.values[colIndex], tableName, colName)
			}
		}
		return col.Append(items)

	case "UInt32":
		items := make([]uint32, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToUInt32(row.values[colIndex], tableName, colName)
			}
		}
		return col.Append(items)

	case "UInt64":
		items := make([]uint64, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToUInt64(row.values[colIndex], tableName, colName)
			}
		}
		return col.Append(items)

	case "DateTime", "DateTime64":
		items := make([]time.Time, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToTime(row.values[colIndex], tableName, colName)
			}
		}
		return col.Append(items)

	case "Map(String,String)":
		items := make([]map[string]string, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToStringMap(row.values[colIndex], tableName, colName)
			}
		}
		return col.Append(items)

	case "Map(String,Float32)":
		items := make([]map[string]float32, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToFloat32Map(row.values[colIndex], tableName, colName)
			}
		}
		return col.Append(items)

	case "Map(String,Float64)":
		items := make([]map[string]float64, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToFloat64Map(row.values[colIndex], tableName, colName)
			}
		}
		return col.Append(items)

	case "Array":
		items := make([][]interface{}, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToArray(row.values[colIndex], tableName, colName)
			}
		}
		return col.Append(items)

	default:
		// For any other unknown types, try to convert to string as fallback
		fmt.Printf("Warning: Unknown column type %s for %s.%s, falling back to string conversion\n", colType, tableName, colName)
		items := make([]string, len(rows))
		if colIndex >= 0 {
			for i, row := range rows {
				items[i] = convertToString(row.values[colIndex])
			}
		}
		return col.Append(items)
	}
}
func SplitWithEscaping(s, separator, escape string) []string {
	s = strings.ReplaceAll(s, escape+separator, "\x00")
	tokens := strings.Split(s, separator)
	for i, token := range tokens {
		tokens[i] = strings.ReplaceAll(token, "\x00", separator)
	}
	return tokens
}
