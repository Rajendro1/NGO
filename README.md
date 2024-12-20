package fileprocessorfactory

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"gecgithub01.walmart.com/CustomerTechnologies/insurancePlanApply/internal/models"
	"github.com/petergtz/pegomock/v4"
)

func TestCarrItemRestrictProcessor_Process(t *testing.T) {
	pegomock.RegisterMockTestingT(t)
	mockRepo := NewMockCarrItemRestrictRepository()
	mockFileUtil := NewMockFileUtilMethods()

	type args struct {
		fileMetadata *models.FileMetadata
	}

	ctx := context.Background()

	type mockOutputs struct {
		fileData [][]string
	}

	resetMocks := func() {
		mockRepo = NewMockCarrItemRestrictRepository()
		mockFileUtil = NewMockFileUtilMethods()
	}

	tests := []struct {
		name                      string
		args                      args
		carrItemRestrictProcessor func(args *models.FileMetadata) *CarrItemRestrictProcessor
		mockOutputs               mockOutputs
		prepareMocks              func(mockOutputs)
		verifyMocks               func()
		want                      bool
		wantErr                   bool
	}{
		{
			name: "Should return true",
			args: args{
				fileMetadata: &models.FileMetadata{
					FileId: 1,
				},
			},
			carrItemRestrictProcessor: func(args *models.FileMetadata) *CarrItemRestrictProcessor {
				return NewCarrItemRestrictProcessor(testLogger{}, mockFileUtil, args, mockRepo, ctx, &sql.Tx{}, 1)
			},
			mockOutputs: mockOutputs{
				fileData: [][]string{{"1", "1", "1", "1", "1", "1"}},
			},
			prepareMocks: func(outputs mockOutputs) {
				pegomock.When(mockFileUtil.ReadUnlFiledata("/hidp_pkg_1_carr_item_restrict.unl")).ThenReturn(outputs.fileData, nil)
				pegomock.When(mockRepo.GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(nil)
				pegomock.When(mockRepo.GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())).ThenReturn(nil)
			},
			verifyMocks: func() {
				mockFileUtil.VerifyWasCalledOnce().ReadUnlFiledata(pegomock.Any[string]())
				mockRepo.VerifyWasCalledOnce().GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "Should return false and error when reading unl file fails",
			args: args{
				fileMetadata: &models.FileMetadata{
					FileId: 1,
				},
			},
			carrItemRestrictProcessor: func(args *models.FileMetadata) *CarrItemRestrictProcessor {
				return NewCarrItemRestrictProcessor(testLogger{}, mockFileUtil, args, mockRepo, ctx, &sql.Tx{}, 1)
			},
			mockOutputs: mockOutputs{
				fileData: [][]string{{"1", "1", "1", "1", "1", "1"}},
			},
			prepareMocks: func(outputs mockOutputs) {
				pegomock.When(mockFileUtil.ReadUnlFiledata("/hidp_pkg_1_carr_item_restrict.unl")).ThenReturn(nil, errors.New("reading unl file failed"))
			},
			verifyMocks: func() {
				mockFileUtil.VerifyWasCalledOnce().ReadUnlFiledata(pegomock.Any[string]())
				mockRepo.VerifyWasCalled(pegomock.Never()).GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())
				mockRepo.VerifyWasCalled(pegomock.Never()).GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "Should return false and error when in valid data is passed",
			args: args{
				fileMetadata: &models.FileMetadata{
					FileId: 1,
				},
			},
			carrItemRestrictProcessor: func(args *models.FileMetadata) *CarrItemRestrictProcessor {
				return NewCarrItemRestrictProcessor(testLogger{}, mockFileUtil, args, mockRepo, ctx, &sql.Tx{}, 1)
			},
			mockOutputs: mockOutputs{
				fileData: [][]string{{"A", "1", "1", "1", "1", "1"}},
			},
			prepareMocks: func(outputs mockOutputs) {
				pegomock.When(mockFileUtil.ReadUnlFiledata("/hidp_pkg_1_carr_item_restrict.unl")).ThenReturn(outputs.fileData, nil)
			},
			verifyMocks: func() {
				mockFileUtil.VerifyWasCalledOnce().ReadUnlFiledata(pegomock.Any[string]())
				mockRepo.VerifyWasCalled(pegomock.Never()).GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())
				mockRepo.VerifyWasCalled(pegomock.Never()).GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "Should return false and error when get item restrict count fails",
			args: args{
				fileMetadata: &models.FileMetadata{
					FileId: 1,
				},
			},
			carrItemRestrictProcessor: func(args *models.FileMetadata) *CarrItemRestrictProcessor {
				return NewCarrItemRestrictProcessor(testLogger{}, mockFileUtil, args, mockRepo, ctx, &sql.Tx{}, 1)
			},
			mockOutputs: mockOutputs{
				fileData: [][]string{{"1", "1", "1", "1", "1", "1"}},
			},
			prepareMocks: func(outputs mockOutputs) {
				pegomock.When(mockFileUtil.ReadUnlFiledata("/hidp_pkg_1_carr_item_restrict.unl")).ThenReturn(outputs.fileData, nil)
				pegomock.When(mockRepo.GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(0, errors.New("get iitem restrict count failed"))
			},
			verifyMocks: func() {
				mockFileUtil.VerifyWasCalledOnce().ReadUnlFiledata(pegomock.Any[string]())
				mockRepo.VerifyWasCalledOnce().GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "Should return false and error when delete carrier item restrict fails",
			args: args{
				fileMetadata: &models.FileMetadata{
					FileId: 1,
				},
			},
			carrItemRestrictProcessor: func(args *models.FileMetadata) *CarrItemRestrictProcessor {
				return NewCarrItemRestrictProcessor(testLogger{}, mockFileUtil, args, mockRepo, ctx, &sql.Tx{}, 1)
			},
			mockOutputs: mockOutputs{
				fileData: [][]string{{"1", "1", "1", "1", "1", "1"}},
			},
			prepareMocks: func(outputs mockOutputs) {
				pegomock.When(mockFileUtil.ReadUnlFiledata("/hidp_pkg_1_carr_item_restrict.unl")).ThenReturn(outputs.fileData, nil)
				pegomock.When(mockRepo.GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(errors.New("deletion failed"))
				pegomock.When(mockRepo.GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())).ThenReturn(nil)
			},
			verifyMocks: func() {
				mockFileUtil.VerifyWasCalledOnce().ReadUnlFiledata(pegomock.Any[string]())
				mockRepo.VerifyWasCalledOnce().GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "Should not delete carr item restrict and continue when item restrict count is 0",
			args: args{
				fileMetadata: &models.FileMetadata{
					FileId: 1,
				},
			},
			carrItemRestrictProcessor: func(args *models.FileMetadata) *CarrItemRestrictProcessor {
				return NewCarrItemRestrictProcessor(testLogger{}, mockFileUtil, args, mockRepo, ctx, &sql.Tx{}, 1)
			},
			mockOutputs: mockOutputs{
				fileData: [][]string{{"1", "1", "1", "1", "1", "1"}},
			},
			prepareMocks: func(outputs mockOutputs) {
				pegomock.When(mockFileUtil.ReadUnlFiledata("/hidp_pkg_1_carr_item_restrict.unl")).ThenReturn(outputs.fileData, nil)
				pegomock.When(mockRepo.GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(0, nil)
				pegomock.When(mockRepo.GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())).ThenReturn(nil)
			},
			verifyMocks: func() {
				mockFileUtil.VerifyWasCalledOnce().ReadUnlFiledata(pegomock.Any[string]())
				mockRepo.VerifyWasCalledOnce().GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "Should not get and delete carr item restrict and continue when same record comes twice",
			args: args{
				fileMetadata: &models.FileMetadata{
					FileId: 1,
				},
			},
			carrItemRestrictProcessor: func(args *models.FileMetadata) *CarrItemRestrictProcessor {
				return NewCarrItemRestrictProcessor(testLogger{}, mockFileUtil, args, mockRepo, ctx, &sql.Tx{}, 1)
			},
			mockOutputs: mockOutputs{
				fileData: [][]string{{"1", "1", "1", "1", "1", "1"}, {"1", "1", "1", "1", "1", "1"}},
			},
			prepareMocks: func(outputs mockOutputs) {
				pegomock.When(mockFileUtil.ReadUnlFiledata("/hidp_pkg_1_carr_item_restrict.unl")).ThenReturn(outputs.fileData, nil)
				pegomock.When(mockRepo.GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(nil)
				pegomock.When(mockRepo.GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())).ThenReturn(nil)
			},
			verifyMocks: func() {
				mockFileUtil.VerifyWasCalledOnce().ReadUnlFiledata(pegomock.Any[string]())
				mockRepo.VerifyWasCalledOnce().GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Twice()).GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Twice()).GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Twice()).InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "Should return false and error when get plan count fails",
			args: args{
				fileMetadata: &models.FileMetadata{
					FileId: 1,
				},
			},
			carrItemRestrictProcessor: func(args *models.FileMetadata) *CarrItemRestrictProcessor {
				return NewCarrItemRestrictProcessor(testLogger{}, mockFileUtil, args, mockRepo, ctx, &sql.Tx{}, 1)
			},
			mockOutputs: mockOutputs{
				fileData: [][]string{{"1", "1", "1", "1", "1", "1"}},
			},
			prepareMocks: func(outputs mockOutputs) {
				pegomock.When(mockFileUtil.ReadUnlFiledata("/hidp_pkg_1_carr_item_restrict.unl")).ThenReturn(outputs.fileData, nil)
				pegomock.When(mockRepo.GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(nil)
				pegomock.When(mockRepo.GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(0, errors.New("get plan count failed"))
			},
			verifyMocks: func() {
				mockFileUtil.VerifyWasCalledOnce().ReadUnlFiledata(pegomock.Any[string]())
				mockRepo.VerifyWasCalledOnce().GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "Should continue when plan count is 0 ",
			args: args{
				fileMetadata: &models.FileMetadata{
					FileId: 1,
				},
			},
			carrItemRestrictProcessor: func(args *models.FileMetadata) *CarrItemRestrictProcessor {
				return NewCarrItemRestrictProcessor(testLogger{}, mockFileUtil, args, mockRepo, ctx, &sql.Tx{}, 1)
			},
			mockOutputs: mockOutputs{
				fileData: [][]string{{"1", "1", "1", "1", "1", "1"}},
			},
			prepareMocks: func(outputs mockOutputs) {
				pegomock.When(mockFileUtil.ReadUnlFiledata("/hidp_pkg_1_carr_item_restrict.unl")).ThenReturn(outputs.fileData, nil)
				pegomock.When(mockRepo.GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(nil)
				pegomock.When(mockRepo.GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(0, nil)
			},
			verifyMocks: func() {
				mockFileUtil.VerifyWasCalledOnce().ReadUnlFiledata(pegomock.Any[string]())
				mockRepo.VerifyWasCalledOnce().GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "Should return false and error when get item count fails",
			args: args{
				fileMetadata: &models.FileMetadata{
					FileId: 1,
				},
			},
			carrItemRestrictProcessor: func(args *models.FileMetadata) *CarrItemRestrictProcessor {
				return NewCarrItemRestrictProcessor(testLogger{}, mockFileUtil, args, mockRepo, ctx, &sql.Tx{}, 1)
			},
			mockOutputs: mockOutputs{
				fileData: [][]string{{"1", "1", "1", "1", "1", "1"}},
			},
			prepareMocks: func(outputs mockOutputs) {
				pegomock.When(mockFileUtil.ReadUnlFiledata("/hidp_pkg_1_carr_item_restrict.unl")).ThenReturn(outputs.fileData, nil)
				pegomock.When(mockRepo.GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(nil)
				pegomock.When(mockRepo.GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())).ThenReturn(0, errors.New("get item count failed"))
			},
			verifyMocks: func() {
				mockFileUtil.VerifyWasCalledOnce().ReadUnlFiledata(pegomock.Any[string]())
				mockRepo.VerifyWasCalledOnce().GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "Should continue when item count is 0",
			args: args{
				fileMetadata: &models.FileMetadata{
					FileId: 1,
				},
			},
			carrItemRestrictProcessor: func(args *models.FileMetadata) *CarrItemRestrictProcessor {
				return NewCarrItemRestrictProcessor(testLogger{}, mockFileUtil, args, mockRepo, ctx, &sql.Tx{}, 1)
			},
			mockOutputs: mockOutputs{
				fileData: [][]string{{"1", "1", "1", "1", "1", "1"}},
			},
			prepareMocks: func(outputs mockOutputs) {
				pegomock.When(mockFileUtil.ReadUnlFiledata("/hidp_pkg_1_carr_item_restrict.unl")).ThenReturn(outputs.fileData, nil)
				pegomock.When(mockRepo.GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(nil)
				pegomock.When(mockRepo.GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())).ThenReturn(0, nil)
			},
			verifyMocks: func() {
				mockFileUtil.VerifyWasCalledOnce().ReadUnlFiledata(pegomock.Any[string]())
				mockRepo.VerifyWasCalledOnce().GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "Should skip insertion when subject area isSubjectAreaItemTpRmpkg(2020) ",
			args: args{
				fileMetadata: &models.FileMetadata{
					FileId: 1,
				},
			},
			carrItemRestrictProcessor: func(args *models.FileMetadata) *CarrItemRestrictProcessor {
				return NewCarrItemRestrictProcessor(testLogger{}, mockFileUtil, args, mockRepo, ctx, &sql.Tx{}, 2020)
			},
			mockOutputs: mockOutputs{
				fileData: [][]string{{"1", "1", "1", "1", "1", "1"}},
			},
			prepareMocks: func(outputs mockOutputs) {
				pegomock.When(mockFileUtil.ReadUnlFiledata("/hidp_pkg_1_carr_item_restrict.unl")).ThenReturn(outputs.fileData, nil)
				pegomock.When(mockRepo.GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(nil)
				pegomock.When(mockRepo.GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())).ThenReturn(1, nil)
			},
			verifyMocks: func() {
				mockFileUtil.VerifyWasCalledOnce().ReadUnlFiledata(pegomock.Any[string]())
				mockRepo.VerifyWasCalledOnce().GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())
				mockRepo.VerifyWasCalled(pegomock.Never()).InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "Should return error when insert carr item restrict fails",
			args: args{
				fileMetadata: &models.FileMetadata{
					FileId: 1,
				},
			},
			carrItemRestrictProcessor: func(args *models.FileMetadata) *CarrItemRestrictProcessor {
				return NewCarrItemRestrictProcessor(testLogger{}, mockFileUtil, args, mockRepo, ctx, &sql.Tx{}, 1)
			},
			mockOutputs: mockOutputs{
				fileData: [][]string{{"1", "1", "1", "1", "1", "1"}},
			},
			prepareMocks: func(outputs mockOutputs) {
				pegomock.When(mockFileUtil.ReadUnlFiledata("/hidp_pkg_1_carr_item_restrict.unl")).ThenReturn(outputs.fileData, nil)
				pegomock.When(mockRepo.GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(nil)
				pegomock.When(mockRepo.GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())).ThenReturn(1, nil)
				pegomock.When(mockRepo.InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())).ThenReturn(errors.New("insertion failed"))
			},
			verifyMocks: func() {
				mockFileUtil.VerifyWasCalledOnce().ReadUnlFiledata(pegomock.Any[string]())
				mockRepo.VerifyWasCalledOnce().GetItemRestrictCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().DeleteCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().GetPlanCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().GetItemCount(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[int]())
				mockRepo.VerifyWasCalledOnce().InsertCarrItemRestrict(pegomock.Any[context.Context](), pegomock.Any[*sql.Tx](), pegomock.Any[models.CarrItemRestrict]())
			},
			want:    false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareMocks(tt.mockOutputs)
			got, err := tt.carrItemRestrictProcessor(tt.args.fileMetadata).Process("")
			if (err != nil) != tt.wantErr {
				t.Errorf("CarrItemRestrictProcessor.Process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CarrItemRestrictProcessor.Process() = %v, want %v", got, tt.want)
			}
			tt.verifyMocks()
			resetMocks()
		})
	}
}

func TestCarrItemRestrictProcessor_NewCarrItemRestrict(t *testing.T) {

	type args struct {
		input []string
	}

	tests := []struct {
		name    string
		args    args
		want    models.CarrItemRestrict
		wantErr bool
	}{
		{
			name: "Should return valid carr item restrict object",
			args: args{
				input: []string{"1", "1", "1", "1", "1", "1"},
			},
			want: models.CarrItemRestrict{
				CarrierCoId:      1,
				InsCarrPlanId:    1,
				CoverageOptionId: 1,
				ItemMdsFamId:     1,
				RestrictTypeCode: 1,
				Restrictionvalue: "1",
			},
			wantErr: false,
		},
		{
			name: "Should return error when invalid data CarrierCoId and InsCarrPlanId is present",
			args: args{
				input: []string{"A", "A", "1", "1", "1", "1"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewCarrItemRestrict(tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCarrItemRestrict error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewCarrItemRestrict = %v, want %v", got, tt.want)
			}
		})
	}
}

// Empty Loggers to avoid having to write log file stuff during test.
type testLogger struct{}

func (tl testLogger) Info(string, ...any) {
	return
}

func (tl testLogger) Warn(string, ...any) {
	return
}

func (tl testLogger) Error(string, ...any) {
	return
}
