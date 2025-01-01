// main.go
package main

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

// SQLExecutor is the interface for executing SQL statements.
type SQLExecutor interface {
	InsertOldSubmnRqst(data *SubmissionRequest) error
	InsertNewSubmnRqst(data *SubmissionRequest) error
	InsertTempSubmnRqst(data *SubmissionRequest) error
}

// SubmissionRequest holds data parsed from the input file.
type SubmissionRequest struct {
	SubmnRqmtCode     int
	SeqNbr            int
	ClaimPartCode     int
	ClaimSegmentCode  int
	GroupID           int
	FieldNbr          int
	FieldID           string
	MaxLengthQty      int
	FieldDefaultTxt   string
	DefaultTypeCode   int
	UsageCode         int
	FieldFormatTxt    string
	SignatureReqInd   string
	PreSeprCharNbr    *int
	PostSeprCharNbr   *int
	FieldIDInclInd    string
}

// Logger interface for logging.
type Logger interface {
	LogInfo(message string)
	LogError(err error)
}

// SimpleLogger logs to stdout.
type SimpleLogger struct{}

func (l *SimpleLogger) LogInfo(message string) {
	log.Println("INFO:", message)
}

func (l *SimpleLogger) LogError(err error) {
	log.Println("ERROR:", err)
}

// FileProcessor processes the input file.
type FileProcessor struct {
	SQLExecutor SQLExecutor
	Logger      Logger
}

// LoadSubmissionRequest processes the file and inserts data into the database.
func (fp *FileProcessor) LoadSubmissionRequest(filePath string, isOld bool, isNew bool, entity int) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCnt := 0

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ",")
		if len(fields) < 15 {
			fp.Logger.LogError(errors.New("insufficient fields in line"))
			continue
		}

		data, err := parseSubmissionRequest(fields)
		if err != nil {
			fp.Logger.LogError(fmt.Errorf("failed to parse line %d: %w", lineCnt, err))
			continue
		}

		if entity == 3001 {
			if isOld {
				if err := fp.SQLExecutor.InsertOldSubmnRqst(data); err != nil {
					fp.Logger.LogError(fmt.Errorf("failed to insert old submission request: %w", err))
					continue
				}
			}
			if isNew {
				if err := fp.SQLExecutor.InsertNewSubmnRqst(data); err != nil {
					fp.Logger.LogError(fmt.Errorf("failed to insert new submission request: %w", err))
					continue
				}
			}
		} else if entity == 3101 {
			if err := fp.SQLExecutor.InsertTempSubmnRqst(data); err != nil {
				fp.Logger.LogError(fmt.Errorf("failed to insert temp submission request: %w", err))
				continue
			}
		}

		lineCnt++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}

	fp.Logger.LogInfo("File processed successfully")
	return nil
}

// parseSubmissionRequest parses a line into a SubmissionRequest.
func parseSubmissionRequest(fields []string) (*SubmissionRequest, error) {
	toInt := func(s string) (int, error) {
		return strconv.Atoi(strings.TrimSpace(s))
	}

	submnRqmtCode, err := toInt(fields[0])
	if err != nil {
		return nil, fmt.Errorf("invalid SubmnRqmtCode: %w", err)
	}
	seqNbr, err := toInt(fields[1])
	if err != nil {
		return nil, fmt.Errorf("invalid SeqNbr: %w", err)
	}
	// Similar parsing for other numeric fields...

	data := &SubmissionRequest{
		SubmnRqmtCode:   submnRqmtCode,
		SeqNbr:          seqNbr,
		FieldID:         fields[6],
		FieldDefaultTxt: fields[8],
		FieldIDInclInd:  fields[14],
	}

	// Optional fields (PreSeprCharNbr, PostSeprCharNbr):
	if fields[12] != "" {
		val, err := toInt(fields[12])
		if err != nil {
			return nil, fmt.Errorf("invalid PreSeprCharNbr: %w", err)
		}
		data.PreSeprCharNbr = &val
	}
	if fields[13] != "" {
		val, err := toInt(fields[13])
		if err != nil {
			return nil, fmt.Errorf("invalid PostSeprCharNbr: %w", err)
		}
		data.PostSeprCharNbr = &val
	}

	return data, nil
}

func main() {
	sqlExecutor := &MySQLExecutor{} // Implement this in sql.go.
	logger := &SimpleLogger{}

	processor := &FileProcessor{
		SQLExecutor: sqlExecutor,
		Logger:      logger,
	}

	filePath := "data.csv"
	if err := processor.LoadSubmissionRequest(filePath, true, false, 3001); err != nil {
		log.Fatalf("failed to load submission request: %v", err)
	}
}
