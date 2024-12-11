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

package destination

import (
	"context"
	"errors"
	"testing"

	"github.com/conduitio-labs/conduit-connector-vitess/destination/mock"
	"github.com/conduitio-labs/conduit-connector-vitess/destination/writer"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

func TestDestination_Configure(t *testing.T) {
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
					ConfigAddress:    "localhost:15991",
					ConfigTable:      "users",
					ConfigKeyColumn:  "id",
					ConfigUsername:   "admin",
					ConfigPassword:   "super_secret",
					ConfigKeyspace:   "test",
					ConfigTabletType: "replica",
				},
			},
			wantErr: false,
		},
		{
			name: "fail, missing address",
			args: args{
				cfg: map[string]string{
					ConfigTable:     "users",
					ConfigKeyColumn: "id",
					ConfigKeyspace:  "test",
				},
			},
			wantErr: true,
		},
		{
			name: "fail, invalid address",
			args: args{
				cfg: map[string]string{
					ConfigAddress:   "localhost:",
					ConfigTable:     "users",
					ConfigKeyColumn: "id",
					ConfigKeyspace:  "test",
				},
			},
			wantErr: true,
		},
		{
			name: "fail, invalid table",
			args: args{
				cfg: map[string]string{
					ConfigAddress:   "localhost:15991",
					ConfigTable:     "ABRATQkOlvPWqfTgUssUuGYCVkQJd4YlkQ1BEe51cctLMqCzjLanlwARrlXZVmd4vbJLne",
					ConfigKeyColumn: "id",
					ConfigKeyspace:  "test",
				},
			},
			wantErr: true,
		},
		{
			name: "fail, missing table",
			args: args{
				cfg: map[string]string{
					ConfigAddress:   "localhost:15991",
					ConfigKeyColumn: "id",
					ConfigKeyspace:  "test",
				},
			},
			wantErr: true,
		},
		{
			name: "fail, invalid key column",
			args: args{
				cfg: map[string]string{
					ConfigAddress:   "localhost:15991",
					ConfigTable:     "users",
					ConfigKeyColumn: "ABRATQkOlvPWqfTgUssUuGYCVkQJd4YlkQ1BEe51cctLMqCzjLanlwARrlXZVmd4vbJLne",
					ConfigKeyspace:  "test",
				},
			},
			wantErr: true,
		},
		{
			name: "fail, missing key column",
			args: args{
				cfg: map[string]string{
					ConfigAddress:  "localhost:15991",
					ConfigTable:    "users",
					ConfigKeyspace: "test",
				},
			},
			wantErr: true,
		},
		{
			name: "fail, missing keyspace",
			args: args{
				cfg: map[string]string{
					ConfigAddress:   "localhost:15991",
					ConfigTable:     "users",
					ConfigKeyColumn: "id",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := &Destination{}
			if err := d.Configure(context.Background(), tt.args.cfg); (err != nil) != tt.wantErr {
				t.Errorf("Destination.Configure() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDestination_Write_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	record := opencdc.Record{
		Position:  opencdc.Position("1.0"),
		Operation: opencdc.OperationCreate,
		Key: opencdc.StructuredData{
			"id": 1,
		},
		Payload: opencdc.Change{
			After: opencdc.StructuredData{
				"id":   1,
				"name": "Void",
			},
		},
	}

	w := mock.NewMockWriter(ctrl)
	w.EXPECT().Write(ctx, record).Return(nil)

	d := Destination{
		writer: w,
	}

	written, err := d.Write(ctx, []opencdc.Record{record})
	is.NoErr(err)
	is.Equal(written, 1)
}

func TestDestination_Write_Fail(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	record := opencdc.Record{
		Position:  opencdc.Position("1.0"),
		Operation: opencdc.OperationCreate,
		Key: opencdc.StructuredData{
			"id": 1,
		},
	}

	w := mock.NewMockWriter(ctrl)
	w.EXPECT().Write(ctx, record).Return(writer.ErrEmptyPayload)

	d := Destination{
		writer: w,
	}

	written, err := d.Write(ctx, []opencdc.Record{record})
	is.Equal(err != nil, true)
	is.Equal(written, 0)
}

func TestDestination_Teardown_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	w := mock.NewMockWriter(ctrl)
	w.EXPECT().Close().Return(nil)

	d := Destination{
		writer: w,
	}

	err := d.Teardown(ctx)
	is.NoErr(err)
}

func TestDestination_Teardown_SuccessWriterIsNil(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	d := Destination{
		writer: nil,
	}

	err := d.Teardown(ctx)
	is.NoErr(err)
}

func TestDestination_Teardown_FailUnexpectedError(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	w := mock.NewMockWriter(ctrl)
	w.EXPECT().Close().Return(errors.New("some error"))

	d := Destination{
		writer: w,
	}

	err := d.Teardown(ctx)
	is.Equal(err != nil, true)
}
