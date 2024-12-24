func (pcp *PhmCarrProcessor) Process(packageDir string) (bool, error) {
	unlFile := fmt.Sprintf(constants.UnlFilePhmCarrProcessor, packageDir, pcp.fileMetadata.FileId)
	pcp.logger.Info("Loading unl file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, pcp.fileMetadata.FileId)

	fileData, err := pcp.fileUtils.ReadUnlFiledata(unlFile)
	if err != nil {
		return false, fmt.Errorf("error loading the unl file: %w", err)
	}

	for _, row := range fileData {
		if err := pcp.processRow(row); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (pcp *PhmCarrProcessor) processRow(row string) error {
	phmCarr, err := NewPhmCarr(row)
	if err != nil {
		return fmt.Errorf("error creating phm carr object: %w", err)
	}

	planExists, err := pcp.planExists(phmCarr)
	if err != nil || !planExists {
		return err
	}

	return pcp.handlePhmCarr(phmCarr)
}

func (pcp *PhmCarrProcessor) planExists(phmCarr *PhmCarr) (bool, error) {
	count, err := pcp.repository.GetPlanCount(pcp.ctx, pcp.tx, phmCarr.CarrierCoId, phmCarr.InsCarrPlanId)
	if err != nil {
		return false, fmt.Errorf("error getting plan count: %w", err)
	}
	if count == 0 {
		return false, fmt.Errorf("skipping phmCarr record processing as plan does not exist with CarrierCoId: %d and InsCarrPlan %d", phmCarr.CarrierCoId, phmCarr.InsCarrPlanId)
	}
	return true, nil
}

func (pcp *PhmCarrProcessor) handlePhmCarr(phmCarr *PhmCarr) error {
	count, err := pcp.repository.GetFamCount(pcp.ctx, pcp.tx, phmCarr.ProcessFamNbr)
	if err != nil {
		return fmt.Errorf("error getting fam count: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("skipping phmCarr record processing as Fam does not exist with ProcessFamNbr %d", phmCarr.ProcessFamNbr)
	}

	return pcp.updatePhmCarrRecords(phmCarr)
}

func (pcp *PhmCarrProcessor) updatePhmCarrRecords(phmCarr *PhmCarr) error {
	if err := pcp.repository.DeletePhmCarr(pcp.ctx, pcp.tx, phmCarr.CarrierCoId, phmCarr.InsCarrPlanId); err != nil {
		return fmt.Errorf("error deleting phm carr: %w", err)
	}
	if err := pcp.repository.InsertPhmCarr(pcp.ctx, pcp.tx, phmCarr); err != nil {
		return fmt.Errorf("error inserting phm carr: %w", err)
	}
	return nil
}
