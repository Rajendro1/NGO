```
package fileprocessorfactory

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"yourproject/internal/constants"
	"yourproject/internal/models"
	"yourproject/internal/utils"
)
type SubmnRqst struct {
    SubmnRqmtCode     int
    SeqNbr            int
    ClaimPartCode     int
    ClaimSegmentCode  int
    GroupId           int
    FieldNbr          int
    FieldId           string
    MaxLengthQty      int
    FieldDefaultTxt   string
    DefaultTypeCode   int
    UsageCode         int
    FieldFormatTxt    string
    SignatureReqInd   string
    PreSepCharNbr     int
    PostSepCharNbr    int
    FieldIdInclInd    string
}
type SubmnRqstProcessor struct {
	ctx        context.Context
	tx         *sql.Tx
	logger     Logger
	repository SubmnRqstRepo
	fileData   SubmnRqstFileData
}

type SubmnRqstFileData struct {
	fileUtils    utils.FileUtilMethods
	fileMetadata models.FileMetadata
	entityId     int
}

type SubmnRqstRepo interface {
	InsertSubmnRqst(ctx context.Context, tx *sql.Tx, submnRqst *models.SubmnRqst) error
	InsertSubmnRqstD0(ctx context.Context, tx *sql.Tx, submnRqst *models.SubmnRqst) error
	InsertSubmnRqstTemp(ctx context.Context, tx *sql.Tx, submnRqst *models.SubmnRqst) error
}

func NewSubmnRqstFileData(fileUtils utils.FileUtilMethods, fileMetadata models.FileMetadata, entityId int) SubmnRqstFileData {
	return SubmnRqstFileData{fileUtils: fileUtils, fileMetadata: fileMetadata, entityId: entityId}
}

func NewSubmnRqstProcessor(ctx context.Context, tx *sql.Tx, logger Logger, repository SubmnRqstRepo, fileData SubmnRqstFileData) *SubmnRqstProcessor {
	return &SubmnRqstProcessor{ctx: ctx, tx: tx, logger: logger, repository: repository, fileData: fileData}
}

func (sp *SubmnRqstProcessor) Process(packageDir string) error {
	unlFile := fmt.Sprintf(constants.UnlFileSubmnRqst, packageDir, sp.fileData.fileMetadata.FileId)

	// Determine processing flags
	cCFVersion := ""
	subjArea := 0
	doOld := GetDoOldValue(cCFVersion, subjArea)
	doNew := GetDoNewValue(cCFVersion, subjArea)

	sp.logger.Info("Loading UNL file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, sp.fileData.fileMetadata.FileId)
	fileData, err := sp.fileData.fileUtils.ReadUnlFiledata(unlFile)
	if err != nil {
		sp.logger.Error("Error loading the UNL file", constants.LoggingKeyUnlFileName, unlFile, slog.Any(constants.ErrorConst, err))
		return err
	}

	submnRqsts, err := GetSubmnRqstList(fileData)
	if err != nil {
		sp.logger.Error("Error creating SubmnRqst object", slog.Any(constants.Rows, fileData), slog.Any(constants.ErrorConst, err))
		return err
	}

	for _, submnRqst := range submnRqsts {
		if sp.fileData.entityId == constants.ClaimFormatEntityCode {
			if doOld {
				if err := sp.repository.InsertSubmnRqst(sp.ctx, sp.tx, submnRqst); err != nil {
					sp.logger.Error("Error inserting SubmnRqst", slog.Any(constants.Row, submnRqst), slog.Any(constants.ErrorConst, err))
					break
				}
			}
			if doNew {
				if err := sp.repository.InsertSubmnRqstD0(sp.ctx, sp.tx, submnRqst); err != nil {
					sp.logger.Error("Error inserting SubmnRqstD0", slog.Any(constants.Row, submnRqst), slog.Any(constants.ErrorConst, err))
					return err
				}
			}
		}
		if sp.fileData.entityId == constants.NsClaimFormatEntityCode {
			if err := sp.repository.InsertSubmnRqstTemp(sp.ctx, sp.tx, submnRqst); err != nil {
				sp.logger.Error("Error inserting SubmnRqstTemp", slog.Any(constants.Row, submnRqst), slog.Any(constants.ErrorConst, err))
				break
			}
		}
	}

	sp.logger.Info(constants.SuccessfullyProcessedFile, constants.LoggingKeyUnlFileName, unlFile, constants.RecordLength, len(fileData))
	return nil
}

func GetSubmnRqstList(rows [][]string) ([]*models.SubmnRqst, error) {
	submnRqsts := make([]*models.SubmnRqst, 0, len(rows))

	for _, entry := range rows {
		if len(entry) < 16 { // Ensure there are enough columns
			return nil, fmt.Errorf("insufficient fields in file entry: %s", entry)
		}

		preSepCharNbr, err := parseNullableInt(entry[13])
		if err != nil {
			return nil, fmt.Errorf("error parsing preSepCharNbr: %w", err)
		}

		postSepCharNbr, err := parseNullableInt(entry[14])
		if err != nil {
			return nil, fmt.Errorf("error parsing postSepCharNbr: %w", err)
		}

		submnRqsts = append(submnRqsts, &models.SubmnRqst{
			SubmnRqmtCode:    mustAtoi(entry[0]),
			SeqNbr:           mustAtoi(entry[1]),
			ClaimPartCode:    mustAtoi(entry[2]),
			ClaimSegmentCode: mustAtoi(entry[3]),
			GroupId:          mustAtoi(entry[4]),
			FieldNbr:         mustAtoi(entry[5]),
			FieldId:          strings.TrimSpace(entry[6]),
			MaxLengthQty:     mustAtoi(entry[7]),
			FieldDefaultTxt:  strings.TrimSpace(entry[8]),
			DefaultTypeCode:  mustAtoi(entry[9]),
			UsageCode:        mustAtoi(entry[10]),
			FieldFormatTxt:   strings.TrimSpace(entry[11]),
			SignatureReqInd:  strings.TrimSpace(entry[12]),
			PreSepCharNbr:    preSepCharNbr,
			PostSepCharNbr:   postSepCharNbr,
			FieldIdInclInd:   strings.TrimSpace(entry[15]),
		})
	}

	return submnRqsts, nil
}
func parseNullableInt(s string) (int, error) {
    if s == "" || strings.ToLower(s) == "null" {
        return 0, nil // Return zero if null; adjust if different logic needed
    }
    return strconv.Atoi(s)
}

// Helper function to handle strconv.Atoi repeats
func mustAtoi(s string) int {
    i, err := strconv.Atoi(s)
    if err != nil {
        log.Fatalf("Error parsing integer from string '%s': %v", s, err)
    }
    return i
}

// Repository Implementation
type SubmnRqstRepository struct{}

func (r *SubmnRqstRepository) InsertSubmnRqstD0(ctx context.Context, tx *sql.Tx, submnRqst *models.SubmnRqst) error {
	query := `INSERT INTO submn_rqst_d0 (submn_rqmt_code, seq_nbr, claim_part_code, claim_segment_code, group_id, usage_code, field_id, field_default_txt)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := tx.ExecContext(ctx, query, submnRqst.SubmnRqmtCode, submnRqst.SeqNbr, submnRqst.ClaimPartCode, submnRqst.ClaimSegmentCode, submnRqst.GroupId, submnRqst.UsageCode, submnRqst.FieldId, submnRqst.FieldDefaultTxt)
	return err
}


// InsertSubmnRqstTemp inserts data into the temporary table submn_rqst_temp
func (r *SubmnRqstRepository) InsertSubmnRqstTemp(ctx context.Context, tx *sql.Tx, submnRqst *models.SubmnRqst) error {
	query := `INSERT INTO submn_rqst_temp (submn_rqmt_code, seq_nbr, claim_part_code, claim_segment_code, group_id, usage_code, field_id, field_default_txt)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := tx.ExecContext(ctx, query, submnRqst.SubmnRqmtCode, submnRqst.SeqNbr, submnRqst.ClaimPartCode, submnRqst.ClaimSegmentCode, submnRqst.GroupId, submnRqst.UsageCode, submnRqst.FieldId, submnRqst.FieldDefaultTxt)
	return err
}

// InsertSubmnRqst inserts data into the main table submn_rqst
func (r *SubmnRqstRepository) InsertSubmnRqst(ctx context.Context, tx *sql.Tx, submnRqst *models.SubmnRqst) error {
	query := `INSERT INTO submn_rqst (submn_rqmt_code, seq_nbr, claim_part_code, claim_segment_code, group_id, usage_code, field_id, field_default_txt)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := tx.ExecContext(ctx, query, submnRqst.SubmnRqmtCode, submnRqst.SeqNbr, submnRqst.ClaimPartCode, submnRqst.ClaimSegmentCode, submnRqst.GroupId, submnRqst.UsageCode, submnRqst.FieldId, submnRqst.FieldDefaultTxt)
	return err
}

// FetchSubmnRqstByCode retrieves a submission request by submn_rqmt_code
func (r *SubmnRqstRepository) FetchSubmnRqstByCode(ctx context.Context, submnRqmtCode int) (*models.SubmnRqst, error) {
	query := `SELECT submn_rqmt_code, seq_nbr, claim_part_code, claim_segment_code, group_id, usage_code, field_id, field_default_txt
		FROM submn_rqst WHERE submn_rqmt_code = ?`
	row := r.db.QueryRowContext(ctx, query, submnRqmtCode)

	var submnRqst models.SubmnRqst
	err := row.Scan(
		&submnRqst.SubmnRqmtCode,
		&submnRqst.SeqNbr,
		&submnRqst.ClaimPartCode,
		&submnRqst.ClaimSegmentCode,
		&submnRqst.GroupId,
		&submnRqst.UsageCode,
		&submnRqst.FieldId,
		&submnRqst.FieldDefaultTxt,
	)
	if err != nil {
		return nil, err
	}
	return &submnRqst, nil
}

// UpdateSubmnRqst updates an existing submission request
func (r *SubmnRqstRepository) UpdateSubmnRqst(ctx context.Context, tx *sql.Tx, submnRqst *models.SubmnRqst) error {
	query := `UPDATE submn_rqst SET seq_nbr = ?, claim_part_code = ?, claim_segment_code = ?, group_id = ?, usage_code = ?, field_id = ?, field_default_txt = ?
		WHERE submn_rqmt_code = ?`
	_, err := tx.ExecContext(ctx, query, submnRqst.SeqNbr, submnRqst.ClaimPartCode, submnRqst.ClaimSegmentCode, submnRqst.GroupId, submnRqst.UsageCode, submnRqst.FieldId, submnRqst.FieldDefaultTxt, submnRqst.SubmnRqmtCode)
	return err
}

// DeleteSubmnRqst removes a submission request by submn_rqmt_code
func (r *SubmnRqstRepository) DeleteSubmnRqst(ctx context.Context, tx *sql.Tx, submnRqmtCode int) error {
	query := `DELETE FROM submn_rqst WHERE submn_rqmt_code = ?`
	_, err := tx.ExecContext(ctx, query, submnRqmtCode)
	return err
}

```
