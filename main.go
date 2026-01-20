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

var validTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

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
	mu     sync.RWMutex
	Schema *DbTableColumnMap
}

func (s *CurrentDbSchema) Get() *DbTableColumnMap {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Schema
}

func (s *CurrentDbSchema) Set(schema *DbTableColumnMap) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Schema = schema
}

type ColumnInfo struct {
	Table    string
	ColType  string
	Name     string
	Position uint64
}

type QueuedPacketList struct {
	Packets          []*ParsedPacket
	OldestPacketTime time.Time
}

func (p *ParsedPacket) PacketKey() string {
	return fmt.Sprintf("%s:%s", p.tableName, strings.Join(p.columns, ","))
}

func main() {
	exitCtx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	dbConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		DialTimeout:     time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		log.Panicln(err)
	}
	defer dbConn.Close()

	schema, err := fetchClickhouseSchema(dbConn)
	if err != nil {
		log.Panicln(err)
	}

	currentSchema := CurrentDbSchema{Schema: &schema}
	packetQueue := make(chan *ParsedPacket, 5000)

	go startUdpServer(exitCtx, packetQueue)
	go startTcpServer(exitCtx, packetQueue)
	go updateDbSchemaThread(exitCtx, dbConn, &currentSchema)

	go func() {
		<-exitCtx.Done()
		log.Println("Exit signal received, flushing batches...")
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runBatchInsertLoop(exitCtx, dbConn, &currentSchema, packetQueue, 30*time.Second, 2000)
	}()
	wg.Wait()
}

func updateDbSchemaThread(ctx context.Context, dbConn driver.Conn, schema *CurrentDbSchema) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if newSchema, err := fetchClickhouseSchema(dbConn); err != nil {
				log.Println("fetchClickhouseSchema error:", err)
			} else {
				schema.Set(&newSchema)
			}
		}
	}
}

func runBatchInsertLoop(ctx context.Context, db clickhouse.Conn, schema *CurrentDbSchema, packets <-chan *ParsedPacket, maxTimeout time.Duration, maxItems int) {
	queues := make(map[string]*QueuedPacketList)
	isExiting := false

	for {
		timer := time.NewTimer(maxTimeout)

	collect:
		for {
			select {
			case pkt, ok := <-packets:
				if !ok {
					isExiting = true
					timer.Stop()
					break collect
				}
				key := pkt.PacketKey()
				q := queues[key]
				if q == nil {
					q = &QueuedPacketList{OldestPacketTime: time.Now()}
					queues[key] = q
				}
				q.Packets = append(q.Packets, pkt)
				if len(q.Packets) >= maxItems {
					timer.Stop()
					break collect
				}

			case <-timer.C:
				break collect

			case <-ctx.Done():
				isExiting = true
				timer.Stop()
				break collect
			}
		}

		currentSchema := schema.Get()
		for key, q := range queues {
			if len(q.Packets) == 0 {
				continue
			}
			age := time.Since(q.OldestPacketTime)
			if !isExiting && age < maxTimeout && len(q.Packets) < maxItems {
				continue
			}

			if currentSchema == nil {
				fmt.Println("Schema not loaded yet, keeping batch in queue")
				continue
			}

			delete(queues, key)
			fmt.Printf("Saving batch of %d to table %s, %d seconds old\n", len(q.Packets), q.Packets[0].tableName, int(age.Seconds()))

			for i := 0; i < 5; i++ {
				if !insertBatch(db, q.Packets, *currentSchema) {
					break
				}
				fmt.Println("Retrying insert in 5 seconds")
				time.Sleep(5 * time.Second)
			}
		}

		if isExiting {
			return
		}
	}
}

func startUdpServer(ctx context.Context, packetQueue chan<- *ParsedPacket) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: 9030, IP: net.ParseIP("0.0.0.0")})
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	log.Println("Listening for UDP on", conn.LocalAddr())

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	var buf [8192]byte
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		n, _, err := conn.ReadFromUDP(buf[:])
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				fmt.Println("UDP read error:", err)
				continue
			}
		}

		if pkt := parsePacket(buf[:n]); pkt != nil {
			packetQueue <- pkt
		}
	}
}

func startTcpServer(ctx context.Context, packetQueue chan<- *ParsedPacket) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{Port: 9030, IP: net.ParseIP("0.0.0.0")})
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	log.Println("Listening for TCP on", listener.Addr())

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				fmt.Println("TCP accept error:", err)
				continue
			}
		}
		go handleTcpConn(conn, packetQueue)
	}
}

func handleTcpConn(conn net.Conn, packetQueue chan<- *ParsedPacket) {
	defer conn.Close()

	header := make([]byte, 2)
	for {
		conn.SetDeadline(time.Now().Add(30 * time.Second))

		if _, err := io.ReadFull(conn, header); err != nil {
			if err != io.EOF {
				if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
					fmt.Println("TCP header read error:", err)
				}
			}
			return
		}

		size := int(binary.BigEndian.Uint16(header))
		if size > 32*1024 {
			fmt.Println("TCP payload too large:", size)
			return
		}

		payload := make([]byte, size)
		if _, err := io.ReadFull(conn, payload); err != nil {
			fmt.Println("TCP payload read error:", err)
			return
		}

		pkt := parsePacket(payload)
		var resp []byte
		if pkt != nil {
			packetQueue <- pkt
			resp = []byte("K")
		} else {
			resp = []byte("E")
		}

		if _, err := conn.Write(resp); err != nil {
			fmt.Println("TCP write error:", err)
			return
		}
	}
}

func fetchClickhouseSchema(dbConn driver.Conn) (DbTableColumnMap, error) {
	fmt.Println("Fetching column info...")

	rows, err := dbConn.Query(context.Background(), `
		SELECT table, position, type, name
		FROM system.columns
		WHERE database = 'default'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(DbTableColumnMap)
	for rows.Next() {
		var table, colType, name string
		var pos uint64
		if err := rows.Scan(&table, &pos, &colType, &name); err != nil {
			return nil, err
		}
		result[table] = append(result[table], ColumnInfo{
			Table:    table,
			Position: pos,
			ColType:  normalizeClickHouseType(colType),
			Name:     name,
		})
	}

	fmt.Println("Fetched columns for", len(result), "tables")
	return result, nil
}

var (
	reDigits         = regexp.MustCompile(`\(\d+\)`)
	reLowCardinality = regexp.MustCompile(`LowCardinality\(([^)]+)\)`)
	reNullable       = regexp.MustCompile(`Nullable\(([^)]+)\)`)
)

func normalizeClickHouseType(colType string) string {
	t := strings.ReplaceAll(colType, " ", "")
	t = reDigits.ReplaceAllString(t, "")
	t = reLowCardinality.ReplaceAllString(t, "$1")
	t = reNullable.ReplaceAllString(t, "$1")
	return t
}

func insertBatch(db driver.Conn, rows []*ParsedPacket, schema DbTableColumnMap) bool {
	tableName := rows[0].tableName
	if !validTableName.MatchString(tableName) {
		fmt.Println("Invalid table name:", tableName)
		return false
	}
	columns := schema[tableName]
	if columns == nil {
		fmt.Println("No schema for table:", tableName)
		return false
	}

	batch, err := db.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s", tableName))
	if err != nil || batch == nil {
		fmt.Println("Failed to prepare batch:", err)
		return true
	}
	defer batch.Abort()

	packetCols := rows[0].columns
	for _, col := range columns {
		idx := findColumnIndex(packetCols, col.Name)
		batchCol := batch.Column(int(col.Position) - 1)
		if batchCol == nil {
			fmt.Println("Could not get column:", tableName, col.Name)
			return false
		}
		if err := appendColumnData(batchCol, col.ColType, rows, idx, tableName, col.Name); err != nil {
			fmt.Println("Column append error:", tableName, col.Name, err)
			return false
		}
	}

	if err := batch.Send(); err != nil {
		fmt.Println("Batch send error:", tableName, err)
		return true
	}
	return false
}

func findColumnIndex(columns []string, name string) int {
	for i, c := range columns {
		if c == name {
			return i
		}
	}
	return -1
}

func getValues(rows []*ParsedPacket, colIndex int) []interface{} {
	if colIndex < 0 {
		return nil
	}
	vals := make([]interface{}, len(rows))
	for i, row := range rows {
		vals[i] = row.values[colIndex]
	}
	return vals
}

func appendColumnData(col driver.BatchColumn, colType string, rows []*ParsedPacket, colIndex int, tableName, colName string) error {
	vals := getValues(rows, colIndex)
	n := len(rows)

	switch colType {
	case "String":
		items := make([]string, n)
		for i, v := range vals {
			if v != nil {
				items[i] = convertToString(v)
			}
		}
		return col.Append(items)

	case "Float32":
		items := make([]float32, n)
		for i, v := range vals {
			if v != nil {
				items[i] = convertToFloat32(v, tableName, colName)
			}
		}
		return col.Append(items)

	case "Float64":
		items := make([]float64, n)
		for i, v := range vals {
			if v != nil {
				items[i] = convertToFloat64(v, tableName, colName)
			}
		}
		return col.Append(items)

	case "Int8":
		items := make([]int8, n)
		for i, v := range vals {
			if v != nil {
				items[i] = convertToInt8(v, tableName, colName)
			}
		}
		return col.Append(items)

	case "Int16":
		items := make([]int16, n)
		for i, v := range vals {
			if v != nil {
				items[i] = convertToInt16(v, tableName, colName)
			}
		}
		return col.Append(items)

	case "Int32":
		items := make([]int32, n)
		for i, v := range vals {
			if v != nil {
				items[i] = convertToInt32(v, tableName, colName)
			}
		}
		return col.Append(items)

	case "Int64":
		items := make([]int64, n)
		for i, v := range vals {
			if v != nil {
				items[i] = convertToInt64(v, tableName, colName)
			}
		}
		return col.Append(items)

	case "UInt8":
		items := make([]uint8, n)
		for i, v := range vals {
			if v != nil {
				items[i] = convertToUInt8(v, tableName, colName)
			}
		}
		return col.Append(items)

	case "UInt16":
		items := make([]uint16, n)
		for i, v := range vals {
			if v != nil {
				items[i] = convertToUInt16(v, tableName, colName)
			}
		}
		return col.Append(items)

	case "UInt32":
		items := make([]uint32, n)
		for i, v := range vals {
			if v != nil {
				items[i] = convertToUInt32(v, tableName, colName)
			}
		}
		return col.Append(items)

	case "UInt64":
		items := make([]uint64, n)
		for i, v := range vals {
			if v != nil {
				items[i] = convertToUInt64(v, tableName, colName)
			}
		}
		return col.Append(items)

	case "DateTime", "DateTime64":
		items := make([]time.Time, n)
		for i, v := range vals {
			if v != nil {
				items[i] = convertToTime(v, tableName, colName)
			}
		}
		return col.Append(items)

	case "Map(String,String)":
		items := make([]map[string]string, n)
		for i, v := range vals {
			if v != nil {
				items[i] = convertToStringMap(v, tableName, colName)
			}
		}
		return col.Append(items)

	case "Map(String,Float32)":
		items := make([]map[string]float32, n)
		for i, v := range vals {
			if v != nil {
				items[i] = convertToFloat32Map(v, tableName, colName)
			}
		}
		return col.Append(items)

	case "Map(String,Float64)":
		items := make([]map[string]float64, n)
		for i, v := range vals {
			if v != nil {
				items[i] = convertToFloat64Map(v, tableName, colName)
			}
		}
		return col.Append(items)

	case "Array(String)":
		items := make([][]string, n)
		for i, v := range vals {
			if v != nil {
				arr := convertToArray(v, tableName, colName)
				strArr := make([]string, len(arr))
				for j, elem := range arr {
					strArr[j] = convertToString(elem)
				}
				items[i] = strArr
			}
		}
		return col.Append(items)

	case "Array(UInt8)":
		items := make([][]uint8, n)
		for i, v := range vals {
			if v != nil {
				arr := convertToArray(v, tableName, colName)
				typed := make([]uint8, len(arr))
				for j, elem := range arr {
					typed[j] = convertToUInt8(elem, tableName, colName)
				}
				items[i] = typed
			}
		}
		return col.Append(items)

	case "Array(Int64)":
		items := make([][]int64, n)
		for i, v := range vals {
			if v != nil {
				arr := convertToArray(v, tableName, colName)
				typed := make([]int64, len(arr))
				for j, elem := range arr {
					typed[j] = convertToInt64(elem, tableName, colName)
				}
				items[i] = typed
			}
		}
		return col.Append(items)

	case "Array(Float64)":
		items := make([][]float64, n)
		for i, v := range vals {
			if v != nil {
				arr := convertToArray(v, tableName, colName)
				typed := make([]float64, len(arr))
				for j, elem := range arr {
					typed[j] = convertToFloat64(elem, tableName, colName)
				}
				items[i] = typed
			}
		}
		return col.Append(items)

	case "Array(Float32)":
		items := make([][]float32, n)
		for i, v := range vals {
			if v != nil {
				arr := convertToArray(v, tableName, colName)
				typed := make([]float32, len(arr))
				for j, elem := range arr {
					typed[j] = convertToFloat32(elem, tableName, colName)
				}
				items[i] = typed
			}
		}
		return col.Append(items)

	case "Array(Int8)":
		items := make([][]int8, n)
		for i, v := range vals {
			if v != nil {
				arr := convertToArray(v, tableName, colName)
				typed := make([]int8, len(arr))
				for j, elem := range arr {
					typed[j] = convertToInt8(elem, tableName, colName)
				}
				items[i] = typed
			}
		}
		return col.Append(items)

	case "Array(Int16)":
		items := make([][]int16, n)
		for i, v := range vals {
			if v != nil {
				arr := convertToArray(v, tableName, colName)
				typed := make([]int16, len(arr))
				for j, elem := range arr {
					typed[j] = convertToInt16(elem, tableName, colName)
				}
				items[i] = typed
			}
		}
		return col.Append(items)

	case "Array(Int32)":
		items := make([][]int32, n)
		for i, v := range vals {
			if v != nil {
				arr := convertToArray(v, tableName, colName)
				typed := make([]int32, len(arr))
				for j, elem := range arr {
					typed[j] = convertToInt32(elem, tableName, colName)
				}
				items[i] = typed
			}
		}
		return col.Append(items)

	case "Array(UInt16)":
		items := make([][]uint16, n)
		for i, v := range vals {
			if v != nil {
				arr := convertToArray(v, tableName, colName)
				typed := make([]uint16, len(arr))
				for j, elem := range arr {
					typed[j] = convertToUInt16(elem, tableName, colName)
				}
				items[i] = typed
			}
		}
		return col.Append(items)

	case "Array(UInt32)":
		items := make([][]uint32, n)
		for i, v := range vals {
			if v != nil {
				arr := convertToArray(v, tableName, colName)
				typed := make([]uint32, len(arr))
				for j, elem := range arr {
					typed[j] = convertToUInt32(elem, tableName, colName)
				}
				items[i] = typed
			}
		}
		return col.Append(items)

	case "Array(UInt64)":
		items := make([][]uint64, n)
		for i, v := range vals {
			if v != nil {
				arr := convertToArray(v, tableName, colName)
				typed := make([]uint64, len(arr))
				for j, elem := range arr {
					typed[j] = convertToUInt64(elem, tableName, colName)
				}
				items[i] = typed
			}
		}
		return col.Append(items)

	default:
		fmt.Printf("Unknown type %s for %s.%s, using string\n", colType, tableName, colName)
		items := make([]string, n)
		for i, v := range vals {
			if v != nil {
				items[i] = convertToString(v)
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
