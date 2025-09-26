package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
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

type DbTableColumnMap = map[string]map[string]ColumnInfo

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
		updateDbSchemaThread(exitCtx, dbConn, currentSchema)
	}()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		createLogsSaveThread(exitCtx, dbConn, currentSchema, queuedUnparsed, 30*time.Second, 2000)
	}()

	wg.Wait()
}

func updateDbSchemaThread(exitCtx context.Context, dbConn driver.Conn, currentSchema CurrentDbSchema) {
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

func createLogsSaveThread(interruptCtx context.Context, clickh clickhouse.Conn, schema CurrentDbSchema, values <-chan *ParsedPacket, maxTimeout time.Duration, maxItems int) {
	isExiting := false

	for {
		var queueByTable = make(map[string][]*ParsedPacket)
		expire := time.After(maxTimeout)

		for {
			select {
			case value, ok := <-values:
				if !ok {
					isExiting = true
					goto sendBatch
				}
				tableKey := value.PacketKey()
				packetList := append(queueByTable[tableKey], value)
				queueByTable[tableKey] = packetList

				if len(packetList) >= maxItems {
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

		for tableKey, rows := range queueByTable {
			if len(rows) == 0 {
				continue
			}

			delete(queueByTable, tableKey)

			fmt.Println("Saving batch of", len(rows), "to table", rows[0].tableName)

			for i := 0; i < 5; i++ {
				shouldRetry := insertIntoDb(clickh, rows, *schema.Schema)
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

		parsed := parsePacket(buf[:rlen])

		select {
		case packetQueue <- parsed:
		case <-exitCtx.Done():
			return
		}
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

		if payloadSize > 1024*8 {
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

		if result[table] == nil {
			result[table] = make(map[string]ColumnInfo)
		}

		result[table][name] = ColumnInfo{
			Table:    table,
			Position: pos,
			ColType:  strings.Replace(strings.Replace(strings.Replace(strings.Replace(colType, "(3)", "", 1), ")", "", 1), "LowCardinality(", "", 1), "Nullable(", "", 1),
			Name:     name,
		}
	}

	fmt.Println("...Fetched columns for", len(result), "tables")

	return result, err
}

func insertIntoDb(dbConn driver.Conn, rows []*ParsedPacket, columnInfo DbTableColumnMap) bool {
	columnNames := rows[0].columns
	tableName := rows[0].tableName
	columns := columnInfo[tableName]

	if columns == nil {
		fmt.Println("No column info found for table", tableName)
		return false
	}

	if len(columns) != len(columnNames) {
		fmt.Println("Column missmatch in", tableName)

		columnNameMap := make(map[string]bool)
		for _, v := range columnNames {
			columnNameMap[v] = true
			if _, ok := columns[v]; !ok {
				fmt.Println("Packet col does not exist in db", tableName, v)
			}
		}

		for key := range columns {
			if _, ok := columnNameMap[key]; !ok {
				fmt.Println("Db column missing from packet", tableName, key)
			}
		}

		return false
	}

	// fmt.Println("Inserting", len(rows), "rows into", tableName, "...")

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

	for colIndex, colName := range columnNames {
		colInfo, ok := columns[colName]

		if ok {
			col := tx.Column(int(colInfo.Position) - 1)

			if col == nil {
				fmt.Println("Col is nil", tableName, colName)
				return true
			}

			// fmt.Println("Converting column", colName, "to", colInfo.ColType)

			switch colInfo.ColType {
			case "Float32":
				items := make([]float32, len(rows))
				for i, row := range rows {
					val := row.values[colIndex]
					switch v := val.(type) {
					case int:
						items[i] = float32(v)
					case int64:
						items[i] = float32(v)
					case float32:
						items[i] = v
					case float64:
						items[i] = float32(v)
					case bool:
						if v {
							items[i] = 1
						} else {
							items[i] = 0
						}
					case string:
						v2, err := strconv.ParseFloat(v, 32)
						if err == nil {
							items[i] = float32(v2)
						} else {
							items[i] = 0
							fmt.Println("Invalid float number", tableName, colName, v)
						}
					default:
						items[i] = 0
						fmt.Println("Invalid col conversion", tableName, colName, v)
					}
				}
				err = col.Append(items)
				if err != nil {
					fmt.Println("Add column error", tableName, err)
					return true
				}
			case "String":
				items := make([]string, len(rows))
				for i, row := range rows {
					val := row.values[colIndex]
					switch v := val.(type) {
					case int:
					case int64:
						items[i] = strconv.FormatInt(int64(v), 10)
					case float32:
					case float64:
						items[i] = strconv.FormatFloat(float64(v), 'f', 3, 64)
					case string:
						items[i] = v
					default:
						items[i] = ""
						fmt.Println("Invalid col conversion", tableName, colName, v)
					}
				}
				err = col.Append(items)
				if err != nil {
					fmt.Println("Add column error", tableName, err)
					return true
				}
			case "DateTime", "DateTime64":
				items := make([]time.Time, len(rows))
				for i, row := range rows {
					val := row.values[colIndex]
					switch v := val.(type) {
					case int:
					case int64:
						items[i] = time.UnixMilli(int64(v))
					case float32:
					case float64:
						items[i] = time.UnixMilli(int64(v))
					case string:
						date, err := time.Parse(time.RFC3339, strings.Replace(v, " ", "T", 1)+"Z")
						if err != nil {
							fmt.Println("Invalid date", tableName, colName, v, err)
						}
						items[i] = date
					default:
						fmt.Println("Invalid col conversion", tableName, colName, v)
					}
				}
				err = col.Append(items)
				if err != nil {
					fmt.Println("Add column error", tableName, err)
					return true
				}
			case "UInt16":
				items := make([]uint16, len(rows))
				for i, row := range rows {
					val := row.values[colIndex]
					switch v := val.(type) {
					case int:
					case int64:
					case float32:
					case float64:
						items[i] = uint16(v)
					case bool:
						if v {
							items[i] = 1
						} else {
							items[i] = 0
						}
					case string:
						v2, err := strconv.ParseUint(v, 10, 16)
						if err == nil {
							items[i] = uint16(v2)
						} else {
							items[i] = 0
							fmt.Println("Invalid uint16 number", tableName, colName, v)
						}
					default:
						items[i] = 0
						fmt.Println("Invalid col conversion", tableName, colName, v)
					}
				}
				err = col.Append(items)
				if err != nil {
					fmt.Println("Add column error", tableName, err)
					return true
				}
			case "UInt32":
				items := make([]uint32, len(rows))
				for i, row := range rows {
					val := row.values[colIndex]
					switch v := val.(type) {
					case int:
					case int64:
					case float32:
					case float64:
						items[i] = uint32(v)
					case bool:
						if v {
							items[i] = 1
						} else {
							items[i] = 0
						}
					case string:
						v2, err := strconv.ParseUint(v, 10, 32)
						if err == nil {
							items[i] = uint32(v2)
						} else {
							items[i] = 0
							fmt.Println("Invalid uint32 number", tableName, colName, v)
						}
					default:
						items[i] = 0
						fmt.Println("Invalid col conversion", tableName, colName, v)
					}
				}
				err = col.Append(items)
				if err != nil {
					fmt.Println("Add column error", tableName, err)
					return true
				}
			case "Int32":
				items := make([]int32, len(rows))
				for i, row := range rows {
					val := row.values[colIndex]
					switch v := val.(type) {
					case int:
					case int64:
					case float32:
					case float64:
						items[i] = int32(v)
					case bool:
						if v {
							items[i] = 1
						} else {
							items[i] = 0
						}
					case string:
						v2, err := strconv.ParseInt(v, 10, 32)
						if err == nil {
							items[i] = int32(v2)
						} else {
							items[i] = 0
							fmt.Println("Invalid int32 number", tableName, colName, v)
						}
					default:
						items[i] = 0
						fmt.Println("Invalid col conversion", tableName, colName, v)
					}
				}
				err = col.Append(items)
				if err != nil {
					fmt.Println("Add column error", tableName, err)
					return true
				}
			case "UInt8":
				items := make([]uint8, len(rows))
				for i, row := range rows {
					val := row.values[colIndex]
					switch v := val.(type) {
					case int:
					case int64:
					case float32:
					case float64:
						items[i] = uint8(v)
					case bool:
						if v {
							items[i] = 1
						} else {
							items[i] = 0
						}
					case string:
						v2, err := strconv.ParseUint(v, 10, 8)
						if err == nil {
							items[i] = uint8(v2)
						} else {
							items[i] = 0
							fmt.Println("Invalid uint8 number", tableName, colName, v)
						}
					default:
						items[i] = 0
						fmt.Println("Invalid col conversion", tableName, colName, v)
					}
				}
				err = col.Append(items)
				if err != nil {
					fmt.Println("Add column error", tableName, err)
					return true
				}
			case "UInt64":
				items := make([]uint64, len(rows))
				for i, row := range rows {
					val := row.values[colIndex]
					switch v := val.(type) {
					case int:
					case int64:
					case float32:
					case float64:
						items[i] = uint64(v)
					case bool:
						if v {
							items[i] = 1
						} else {
							items[i] = 0
						}
					case string:
						v2, err := strconv.ParseUint(v, 10, 64)
						if err == nil {
							items[i] = uint64(v2)
						} else {
							items[i] = 0
							fmt.Println("Invalid uint64 number", tableName, colName, v)
						}
					default:
						items[i] = 0
						fmt.Println("Invalid col conversion", tableName, colName, v)
					}
				}
				err = col.Append(items)
				if err != nil {
					fmt.Println("Add column error", tableName, err)
					return true
				}
			default:
				fmt.Println("Unknown column type", tableName, colInfo.ColType)
				return true
			}
		} else {
			fmt.Println("Column does not exist in db table", tableName, colName)
		}
	}

	// fmt.Println("Sending...")

	err = tx.Send()

	if err != nil {
		fmt.Println("Commit error", tableName, err)
		return true
	}

	fmt.Println("Saved", len(rows), "rows to", tableName)
	return false
}

func SplitWithEscaping(s, separator, escape string) []string {
	s = strings.ReplaceAll(s, escape+separator, "\x00")
	tokens := strings.Split(s, separator)
	for i, token := range tokens {
		tokens[i] = strings.ReplaceAll(token, "\x00", separator)
	}
	return tokens
}

var regColumnName, regErr = regexp.Compile("[^a-zA-Z_]+")

func parseOldPacket(x string) *ParsedPacket {
	if regErr != nil {
		log.Fatal(regErr)
		return nil
	}

	if !strings.HasSuffix(x, "\n") {
		fmt.Println("No newline at end")
		return nil
	}

	x = strings.TrimSuffix(x, "\n")

	parts := SplitWithEscaping(x, ",", "\\")

	tableName := regColumnName.ReplaceAllString(parts[0], "")

	if tableName == "" {
		fmt.Println("Table name is empty")
		return nil
	}

	isColumn := true
	columnName := ""
	items := make([]ColValue, 0, (len(parts)-1)/2)

	for _, v := range parts[1:] {
		isString := strings.HasPrefix(v, "\"")

		v = strings.ReplaceAll(v, "\\\"", "\x00")
		v = strings.ReplaceAll(v, "\"", "")
		v = strings.ReplaceAll(v, "\x00", "\"")

		if isColumn {
			columnName = regColumnName.ReplaceAllString(v, "")
		} else {
			if isString {
				items = append(items, ColValue{column: columnName, value: v})
			} else if v == "t" {
				items = append(items, ColValue{column: columnName, value: true})
			} else if v == "f" {
				items = append(items, ColValue{column: columnName, value: false})
			} else if v == "n" {
				items = append(items, ColValue{column: columnName, value: nil})
			} else {
				parsed, err := strconv.ParseFloat(v, 32)
				if err == nil {
					items = append(items, ColValue{column: columnName, value: parsed})
				} else {
					fmt.Println("Old format col error", tableName, columnName)
				}
			}
		}
		isColumn = !isColumn
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].column < items[j].column
	})

	columns := make([]string, 0, len(items))
	for _, v := range items {
		columns = append(columns, v.column)
	}

	values := make([]interface{}, 0, len(items))
	for _, v := range items {
		values = append(values, v.value)
	}

	return &ParsedPacket{
		tableName: tableName,
		columns:   columns,
		values:    values,
	}
}

func parsePacket(x []byte) *ParsedPacket {
	if x[0] != '{' {
		//fmt.Println("Old log format:", string(x[:150]))
		return parseOldPacket(string(x))
	}

	var arbitrary_json map[string]interface{}
	err := json.Unmarshal([]byte(x), &arbitrary_json)

	if err != nil {
		fmt.Println("Parse packet error", err)
		return nil
	}

	if arbitrary_json == nil {
		fmt.Println("Parse json returned nil")
		return nil
	}

	tableName, tableNameOk := arbitrary_json["_t"].(string)
	delete(arbitrary_json, "_t")

	if !tableNameOk || tableName == "" {
		fmt.Println("Table name is empty")
		return nil
	}

	items := make([]ColValue, 0, len(arbitrary_json))

	for columnName, v := range arbitrary_json {
		items = append(items, ColValue{column: columnName, value: v})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].column < items[j].column
	})

	columns := make([]string, 0, len(items))
	for _, v := range items {
		columns = append(columns, v.column)
	}

	values := make([]interface{}, 0, len(items))
	for _, v := range items {
		values = append(values, v.value)
	}

	return &ParsedPacket{
		tableName: tableName,
		columns:   columns,
		values:    values,
	}
}
