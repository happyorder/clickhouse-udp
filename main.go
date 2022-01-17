package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go"
)

func main() {
	dbConn, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?username=default&database=default")
	if err != nil {
		log.Fatal(err)
	}
	if err := dbConn.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	defer dbConn.Close()

	addr := net.UDPAddr{
		Port: 9030,
		IP:   net.ParseIP("0.0.0.0"),
	}
	udpConn, err := net.ListenUDP("udp", &addr) // code does not block here
	if err != nil {
		panic(err)
	}
	defer udpConn.Close()

	regColumnName, err := regexp.Compile("[^a-zA-Z_]+")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Listening on", udpConn.LocalAddr())

	queued := make(map[string][]ParsedPacket)
	lastSend := time.Now()

	var buf [2048]byte
	for {
		rlen, _, err := udpConn.ReadFromUDP(buf[:])

		if err != nil {
			fmt.Println("read error", err)
			continue
		}

		// log.Println("Received", rlen, "bytes from", remote, "error:", err)

		x := string(buf[:rlen])

		// fmt.Println(x)
		packet := parsePacket(x, regColumnName)

		if packet == nil {
			return
		}

		key := packet.PacketKey()
		queued[key] = append(queued[key], *packet)

		now := time.Now()
		if (now.Sub(lastSend)) > (time.Second * 5) {
			// fmt.Println("Flushing queue of size", len(queued))
			lastSend = now
			for key, queueSet := range queued {
				go func(innerQueueSet []ParsedPacket) {
					insertIntoDb(dbConn, innerQueueSet)
				}(queueSet)

				delete(queued, key)
			}

			for range queued {
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

func parsePacket(x string, regColumnName *regexp.Regexp) *ParsedPacket {
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
				parsed, _ := strconv.ParseFloat(v, 32)
				items = append(items, ColValue{column: columnName, value: parsed})
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

func insertIntoDb(dbConn *sql.DB, packets []ParsedPacket) {
	tx, err := dbConn.Begin()

	if err != nil {
		fmt.Println("Could not start transaction", err)
		return
	}

	tableName := packets[0].tableName
	columns := strings.Join(packets[0].columns, ",")
	questionMarks := strings.Repeat("?,", len(columns)-1) + "?"

	// fmt.Printf("Saving %d items in table %s, col: %s \n", len(packets), tableName, columns)

	stmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, questionMarks))

	if err != nil {
		fmt.Println("invalid columns", tableName, columns, err)
		return
	}

	defer stmt.Close()

	for _, packet := range packets {
		_, err = stmt.Exec(packet.values...)

		if err != nil {
			fmt.Println("Insert error", err)
		}
	}

	err = tx.Commit()

	if err != nil {
		fmt.Println("commit error", err)
		return
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
