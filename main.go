package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func startUdpServer(packetQueue chan<- *ParsedPacket) {
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

	var buf [1024 * 8]byte
	for {
		rlen, _, err := conn.ReadFromUDP(buf[:])

		if err != nil {
			fmt.Println("read error", err)
			continue
		}

		// log.Println("Received", rlen, "bytes from", remote, "error:", err)

		parsed := parsePacket(buf[:rlen])
		if parsed != nil {
			packetQueue <- parsed
		}
	}
}

func startTcpServer(packetQueue chan<- *ParsedPacket) {
	addr := net.TCPAddr{
		Port: 9030,
		IP:   net.ParseIP("0.0.0.0"),
	}
	conn, err := net.ListenTCP("tcp", &addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	log.Println("Listening for TCP on", conn.Addr().String())

	for {
		// Listen for an incoming connection.
		conn, err := conn.Accept()
		if err != nil {
			fmt.Println("Error accepting")
			panic(err)
		}

		fmt.Println("New tcp client")

		go handleTcpConn(conn, packetQueue)
	}
}

func handleTcpConn(conn net.Conn, packetQueue chan<- *ParsedPacket) {
	defer conn.Close()
	const DEADLINE = 30

	headerBuff := make([]byte, 2)
	payloadBuff := make([]byte, 1024*8)
	OK := []byte("K")

	for {
		conn.SetDeadline(time.Now().Add(DEADLINE * time.Second))

		headerLen, err := conn.Read(headerBuff)
		if err != nil {
			fmt.Println("Error reading tcp header:", err.Error())
			return
		}

		if headerLen != 2 {
			fmt.Println("Tcp header was only", headerLen, "bytes")
			return
		}

		payloadSize := int(binary.BigEndian.Uint16(headerBuff))

		if payloadSize > len(payloadBuff) {
			fmt.Println("Tcp payload too big", payloadSize)
			return
		}

		fmt.Println("Got tcp header with size", payloadSize)

		bytesRead, err := io.ReadAtLeast(conn, payloadBuff, (payloadSize))

		fmt.Println("Got tcp payload with size", bytesRead)

		if bytesRead == payloadSize {
			parsed := parsePacket(payloadBuff[:payloadSize])
			if parsed != nil {
				packetQueue <- parsed
			}
		}

		if err != nil {
			fmt.Println("Tcp read error", err)
			return
		}

		conn.Write(OK)
	}
}

func main() {
	var ctx = context.Background()
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
		log.Fatal(err)
	}
	defer dbConn.Close()

	if err := dbConn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	queuedForSendingMap := make(map[string][]ParsedPacket)
	queuedUnparsed := make(chan *ParsedPacket, 100)
	lastSend := time.Now()
	columnInfo := make(ColumnByTableAndName)

	go func() {
		startUdpServer(queuedUnparsed)
	}()

	go func() {
		startTcpServer(queuedUnparsed)
	}()

	for packet := range queuedUnparsed {
		key := packet.PacketKey()
		queuedForSendingMap[key] = append(queuedForSendingMap[key], *packet)

		now := time.Now()
		if (now.Sub(lastSend)) > (time.Second * 5) {
			// fmt.Println("Flushing queue of size", len(queuedForSendingMap))
			lastSend = now
			for key, queueSet := range queuedForSendingMap {
				if len(columnInfo) == 0 {
					columnInfo, err = fetchColumnOrder(dbConn)

					if err != nil {
						fmt.Println("Could not get columns", err)
					}
				}

				if len(columnInfo) > 0 {
					go func(innerQueueSet []ParsedPacket) {
						insertIntoDb(dbConn, innerQueueSet, columnInfo)
					}(queueSet)
				}

				delete(queuedForSendingMap, key)
			}

			for range queuedForSendingMap {
				fmt.Println("QUEUE IS NOT EMPTY AFTER FLUSH")
			}
		}

	}
}

type ParsedPacket struct {
	tableName string
	columns   []string
	values    []interface{}
}

type ColValue struct {
	column string
	value  interface{}
}

func (p *ParsedPacket) PacketKey() string {
	return fmt.Sprintf("%s:%s", p.tableName, strings.Join(p.columns, ","))
}

func parsePacket(x []byte) *ParsedPacket {
	if x[0] != '{' {
		//fmt.Println("Old log format:", string(x[:150]))
		return nil
	}

	var arbitrary_json map[string]interface{}
	err := json.Unmarshal([]byte(x), &arbitrary_json)

	if err != nil {
		fmt.Println("Parse packet error", err)
		return nil
	}

	tableName := arbitrary_json["_t"].(string)
	delete(arbitrary_json, "_t")

	if tableName == "" {
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

type ColumnByTableAndName = map[string]map[string]ColumnInfo

type ColumnInfo struct {
	Table    string
	ColType  string
	Name     string
	Position uint64
}

func fetchColumnOrder(dbConn driver.Conn) (ColumnByTableAndName, error) {
	fmt.Println("Fetching column info...")

	rows, err := dbConn.Query(context.Background(), `
SELECT table, position, type, name
FROM system.columns
WHERE database = 'default'`)

	result := make(ColumnByTableAndName)

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
			ColType:  strings.Replace(strings.Replace(strings.Replace(colType, ")", "", 1), "LowCardinality(", "", 1), "Nullable(", "", 1),
			Name:     name,
		}
	}

	fmt.Println("...Fetched columns for", len(result), "tables")

	return result, err
}

func insertIntoDb(dbConn driver.Conn, rows []ParsedPacket, columnInfo ColumnByTableAndName) {
	columnNames := rows[0].columns
	tableName := rows[0].tableName
	columns := columnInfo[tableName]

	if columns == nil {
		fmt.Println("No column info found for table", tableName)
		return
	}

	// fmt.Println("Inserting", len(rows), "rows into", tableName, "...")

	tx, err := dbConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s", tableName))

	if err != nil {
		fmt.Println("Could not start transaction", err)
		return
	}

	defer tx.Abort()

	for colIndex, colName := range columnNames {
		colInfo, ok := columns[colName]

		if ok {
			col := tx.Column(int(colInfo.Position) - 1)

			if col == nil {
				fmt.Println("Col is nil", tableName, colName)
				return
			}

			// fmt.Println("Converting column", colName, "to", colInfo.ColType)

			if colInfo.ColType == "Float32" {
				items := make([]float32, len(rows))
				for i, row := range rows {
					val := row.values[colIndex]
					switch v := val.(type) {
					case int:
					case int64:
					case float32:
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
					return
				}
			} else if colInfo.ColType == "String" {
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
					return
				}
			} else if colInfo.ColType == "DateTime" {
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
					return
				}
			} else {
				fmt.Println("Unknown column type", tableName, colInfo.ColType)
				return
			}
		} else {
			fmt.Println("Column does not exist in db table", tableName, colName)
		}
	}

	// fmt.Println("Sending...")

	err = tx.Send()

	if err != nil {
		fmt.Println("Commit error", tableName, err)
		return
	}

	// fmt.Println("Saved", len(rows), "columns to", tableName)
}

func SplitWithEscaping(s, separator, escape string) []string {
	s = strings.ReplaceAll(s, escape+separator, "\x00")
	tokens := strings.Split(s, separator)
	for i, token := range tokens {
		tokens[i] = strings.ReplaceAll(token, "\x00", separator)
	}
	return tokens
}
