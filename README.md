func (cir *CarrItemRestrictProcessor) Process(packageDir string) (bool, error) {
    unlFile := fmt.Sprintf(constants.UnlFileCarrierItemRestrict, packageDir, cir.fileMetadata.FileId)
    cir.logger.Info("Loading unl file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, cir.fileMetadata.FileId)

    fileData, err := cir.fileUtils.ReadUnlFiledata(unlFile)
    if err != nil {
        cir.logger.Error("Error loading the unl file", constants.LoggingKeyUnlFileName, unlFile)
        return false, err
    }

    return cir.processFileData(fileData)
}

func (cir *CarrItemRestrictProcessor) processFileData(fileData []string) (bool, error) {
    var previousCarrItemRestrict *CarrItemRestrict

    for _, row := range fileData {
        carrItemRestrict, err := NewCarrItemRestrict(row)
        if err != nil {
            cir.logger.Error("Error creating carr item restrict object", "row", row)
            return false, err
        }

        if !cir.isNewRecord(previousCarrItemRestrict, carrItemRestrict) {
            continue
        }

        if err := cir.handleNewItemRestrict(carrItemRestrict); err != nil {
            return false, err
        }

        previousCarrItemRestrict = carrItemRestrict
    }

    return true, nil
}

func (cir *CarrItemRestrictProcessor) isNewRecord(previous, current *CarrItemRestrict) bool {
    return previous == nil || 
           previous.CarrierCoId != current.CarrierCoId ||
           previous.InsCarrPlanId != current.InsCarrPlanId ||
           previous.CoverageOptionId != current.CoverageOptionId ||
           previous.ItemMdsFamId != current.ItemMdsFamId
}

func (cir *CarrItemRestrictProcessor) handleNewItemRestrict(carrItemRestrict *CarrItemRestrict) error {
    if count, err := cir.repository.GetItemRestrictCount(cir.ctx, cir.tx, carrItemRestrict.CarrierCoId, carrItemRestrict.InsCarrPlanId, carrItemRestrict.CoverageOptionId, carrItemRestrict.ItemMdsFamId); err != nil {
        return err
    } else if count > 0 {
        return cir.repository.DeleteCarrItemRestrict(cir.ctx, cir.tx, carrItemRestrict.CarrierCoId, carrItemRestrict.InsCarrPlanId, carrItemRestrict.CoverageOptionId, carrItemRestrict.ItemMdsFamId)
    }

    return cir.validateAndInsertItemRestrict(carrItemRestrict)
}

func (cir *CarrItemRestrictProcessor) validateAndInsertItemRestrict(carrItemRestrict *CarrItemRestrict) error {
    if count, err := cir.repository.GetPlanCount(cir.ctx, cir.tx, carrItemRestrict.CarrierCoId, carrItemRestrict.InsCarrPlanId); err != nil || count <= 0 {
        return err
    }
    if count, err := cir.repository.GetItemCount(cir.ctx, cir.tx, carrItemRestrict.ItemMdsFamId); err != nil || count <= 0 {
        return err
    }

    if cir.subjectArea != constants.SubjectAreaItemTpRmpkg {
        return cir.repository.InsertCarrItemRestrict(cir.ctx, cir.tx, carrItemRestrict)
    }
    return nil
}
