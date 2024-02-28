package threethreadgroups

import (
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

    data := make([]byte, fileBufSize)

    chs := make([]chan string, numWriteThreads)
    for i := 0; i < numWriteThreads; i++ {
        chs[i] = make(chan string, 1024)
    }

    buf := make(chan []byte, numParseThreads)
   
    go func() {
        beg := 0
        for { 
            n, _ := temps.Read(data[beg:])
            if n == 0 {
                break
            }

            n += beg

            new_beg := 0
            for data[n - new_beg - 1] != '\n' {
                new_beg += 1
            }

            buf <- append([]byte{}, data[:n - new_beg]...)

            copy(data[:new_beg], data[n - new_beg:n])
            beg = new_beg
        }
        close(buf)
    }()

    var wgR sync.WaitGroup

    for i := 0; i < numParseThreads; i++ {
        wgR.Add(1)

        go func() {
            row := make([]byte, 100)

            for batch := range buf {
                start := 0

                for idx := range batch {
                    if batch[idx] != '\n' {
                        continue
                    }

                    if idx - start > cap(row) {
                        row = make([]byte, (idx - start) * 2)
                    }

                    copy(row, batch[start:idx])
                    chs[int(row[0]) % numWriteThreads] <- string(row[:idx - start])
                    //chs[int(batch[start]) % numWriteThreads] <- string(append([]byte{}, batch[start:idx]...))

                    start = idx + 1
                }
            }
            wgR.Done()
        }()
    }

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
                fmt.Printf("%s=%f/%f/%f\n", name, station.min, station.sum / station.count, station.max)
            }
            wg.Done()
        }(i)
    }

    wgR.Wait()

    for _, ch := range chs {
        close(ch)
    }

    wg.Wait()
}

