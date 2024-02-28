package naive

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Station struct {
    sum float64
    count float64
    min float64
    max float64
}

func Parse(path string, fileBufSize int, numWriteThreads int, numParseThreads int) {
    temps, err := os.Open(path)
    if err != nil {
        panic(err)
    }
    defer temps.Close()

    scanner := bufio.NewScanner(temps)

    chs := make([]chan string, numWriteThreads)
    for i := 0; i < numWriteThreads; i++ {
        chs[i] = make(chan string, 1024)
    }

    go func() {
        for scanner.Scan() {
            row := scanner.Text()

            chs[int(row[0]) % numWriteThreads] <- row
        }

        for _, ch := range chs {
            close(ch)
        }
    }()
   

    var wg sync.WaitGroup

    for i := 0; i < numWriteThreads; i++ {
        wg.Add(1)

        go func(ord int) {
            curMap := map[string]Station{}

            for row := range chs[ord] {
                s := strings.Split(row, ";")

                name := s[0]
                temperature := strings.Split(s[1], ".")

                integer, err := strconv.ParseInt(temperature[0], 10, 64)
                if err != nil {
                    panic(err)
                }
                decimal, err := strconv.ParseInt(temperature[1], 10, 64)
                if err != nil {
                    panic(err)
                }

                temp := float64(integer) + float64(decimal) / 10

                if s, ok := curMap[name]; ok {
                    s.sum += temp
                    s.count++
                    s.min = min(temp, s.min)
                    s.max = max(temp, s.max)
                    curMap[name] = s
                } else {
                    s = Station{}
                    s.min = temp
                    s.max = temp
                    s.count = 1
                    s.sum = temp
                    curMap[name] = s
                }
            }
            for name, station := range curMap {
                fmt.Printf("%s=%f/%f/%f\n", name, station.min, float64(station.sum) / float64(station.count), station.max)
            }
            wg.Done()
        }(i)
    }

    wg.Wait()
}
