// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package source

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-vitess/config"
	"github.com/conduitio-labs/conduit-connector-vitess/source/mock"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
)

func TestSource_Configure(t *testing.T) {
	t.Parallel()

	type args struct {
		cfg map[string]string
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "success",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:       "localhost:15991",
					config.KeyTable:         "users",
					config.KeyKeyColumn:     "id",
					config.KeyUsername:      "admin",
					config.KeyPassword:      "super_secret",
					config.KeyKeyspace:      "test",
					config.KeyTabletType:    "primary",
					ConfigKeyOrderingColumn: "id",
				},
			},
			wantErr: false,
		},
		{
			name: "fail, missing orderingColumn",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:    "localhost:15991",
					config.KeyTable:      "users",
					config.KeyKeyColumn:  "id",
					config.KeyUsername:   "admin",
					config.KeyPassword:   "super_secret",
					config.KeyKeyspace:   "test",
					config.KeyTabletType: "primary",
				},
			},
			wantErr: true,
		},
		{
			name: "fail, invalid orderingColumn, max",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:    "localhost:15991",
					config.KeyTable:      "users",
					config.KeyKeyColumn:  "id",
					config.KeyUsername:   "admin",
					config.KeyPassword:   "super_secret",
					config.KeyKeyspace:   "test",
					config.KeyTabletType: "primary",
					ConfigKeyOrderingColumn: "veeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" +
						"eeeeeeeeeeeeeeeeeeeeeeerylongcolumnname",
				},
			},
			wantErr: true,
		},
		{
			name: "fail, invalid columns, max",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:       "localhost:15991",
					config.KeyTable:         "users",
					config.KeyKeyColumn:     "id",
					config.KeyUsername:      "admin",
					config.KeyPassword:      "super_secret",
					config.KeyKeyspace:      "test",
					config.KeyTabletType:    "primary",
					ConfigKeyOrderingColumn: "id",
					ConfigKeyColumns: "id,veeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" +
						"eeeeeeeeeeeeeeeeeeeeeeerylongcolumnname",
				},
			},
			wantErr: true,
		},
		{
			name: "fail, invalid batchSize, gte",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:       "localhost:15991",
					config.KeyTable:         "users",
					config.KeyKeyColumn:     "id",
					config.KeyUsername:      "admin",
					config.KeyPassword:      "super_secret",
					config.KeyKeyspace:      "test",
					config.KeyTabletType:    "primary",
					ConfigKeyOrderingColumn: "id",
					ConfigKeyBatchSize:      "0",
				},
			},
			wantErr: true,
		},
		{
			name: "fail, invalid batchSize, lte",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:       "localhost:15991",
					config.KeyTable:         "users",
					config.KeyKeyColumn:     "id",
					config.KeyUsername:      "admin",
					config.KeyPassword:      "super_secret",
					config.KeyKeyspace:      "test",
					config.KeyTabletType:    "primary",
					ConfigKeyOrderingColumn: "id",
					ConfigKeyBatchSize:      "1000000",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := &Source{}
			if err := d.Configure(context.Background(), tt.args.cfg); (err != nil) != tt.wantErr {
				t.Errorf("Destination.Configure() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSource_ReadSuccess(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	st := make(sdk.StructuredData)
	st["key"] = "value"

	record := sdk.Record{
		Position:  sdk.Position(`{"last_processed_element_value": 1}`),
		Metadata:  nil,
		CreatedAt: time.Time{},
		Key:       st,
		Payload:   st,
	}

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, nil)
	it.EXPECT().Next(ctx).Return(record, nil)

	s := Source{
		iterator: it,
	}

	r, err := s.Read(ctx)
	is.NoErr(err)

	is.Equal(r, record)
}

func TestSource_ReadFailHasNext(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, errors.New("get data: fail"))

	s := Source{
		iterator: it,
	}

	_, err := s.Read(ctx)
	is.Equal(err != nil, true)
}

func TestSource_ReadFailNext(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, nil)
	it.EXPECT().Next(ctx).Return(sdk.Record{}, errors.New("key is not exist"))

	s := Source{
		iterator: it,
	}

	_, err := s.Read(ctx)
	is.Equal(err != nil, true)
}

func TestSource_TeardownSuccess(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().Stop(ctx).Return(nil)

	s := Source{
		iterator: it,
	}

	err := s.Teardown(ctx)
	is.NoErr(err)
}

func TestSource_TeardownFail(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().Stop(ctx).Return(errors.New("some error"))

	s := Source{
		iterator: it,
	}

	err := s.Teardown(ctx)
	is.Equal(err != nil, true)
}
