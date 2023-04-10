// Code generated by MockGen. DO NOT EDIT.
// Source: destination/destination.go

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	sdk "github.com/conduitio/conduit-connector-sdk"
	gomock "github.com/golang/mock/gomock"
)

// MockWriter is a mock of Writer interface.
type MockWriter struct {
	ctrl     *gomock.Controller
	recorder *MockWriterMockRecorder
}

// MockWriterMockRecorder is the mock recorder for MockWriter.
type MockWriterMockRecorder struct {
	mock *MockWriter
}

// NewMockWriter creates a new mock instance.
func NewMockWriter(ctrl *gomock.Controller) *MockWriter {
	mock := &MockWriter{ctrl: ctrl}
	mock.recorder = &MockWriterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockWriter) EXPECT() *MockWriterMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockWriter) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockWriterMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockWriter)(nil).Close))
}

// Write mocks base method.
func (m *MockWriter) Write(ctx context.Context, record sdk.Record) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", ctx, record)
	ret0, _ := ret[0].(error)
	return ret0
}

// Write indicates an expected call of Write.
func (mr *MockWriterMockRecorder) Write(ctx, record interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockWriter)(nil).Write), ctx, record)
}
