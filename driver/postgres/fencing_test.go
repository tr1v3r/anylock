package postgres

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/tr1v3r/anylock"
)

func TestFencingSchemaAndSequenceOption(t *testing.T) {
	d, mock := mockDriver(t, "", WithFenceSequence("App.Fences"))
	mock.ExpectExec("CREATE TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`CREATE SEQUENCE IF NOT EXISTS "App"."Fences" AS bigint NO CYCLE CACHE 1`)).WillReturnResult(sqlmock.NewResult(0, 0))
	if operationErr := d.EnsureFencingSchema(context.Background()); operationErr != nil {
		t.Fatal(operationErr)
	}
	for _, name := range []string{"", "pg_temp.fences", "x;DROP SEQUENCE y"} {
		if driver, operationErr := New(noIODatabase(t), "", WithFenceSequence(name)); driver != nil || operationErr == nil {
			t.Fatal(name, driver, operationErr)
		}
	}
}

func TestFencedAcquisitionResults(t *testing.T) {
	failure := errors.New("query failed")
	for _, tc := range []struct {
		name    string
		token   int64
		empty   bool
		err     error
		wantErr bool
	}{
		{name: "success", token: 123},
		{name: "busy", empty: true},
		{name: "zero", token: 0, wantErr: true},
		{name: "negative", token: -1, wantErr: true},
		{name: "backend failure", err: failure, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, mock := mockDriver(t, "ns:")
			// nextval is in the outer SELECT fed by the successful modifying CTE,
			// never in INSERT VALUES (which runs before the ON CONFLICT row-lock wait).
			expected := `WITH acquired AS \(INSERT INTO .* RETURNING key\) SELECT pg_catalog.nextval\(\$4::pg_catalog.regclass\) FROM acquired`
			expectation := mock.ExpectQuery(expected).WithArgs("ns:key", "owner", "0 hours 0 minutes 1 seconds 0 microseconds", `"public"."anylock_fences"`)
			if tc.err != nil {
				expectation.WillReturnError(tc.err)
			} else {
				rows := sqlmock.NewRows([]string{"nextval"})
				if !tc.empty {
					rows.AddRow(tc.token)
				}
				expectation.WillReturnRows(rows)
			}
			result, err := d.TryAcquire(context.Background(), "key", "owner", anylock.AcquireOptions{TTL: time.Second, Fencing: true})
			if (err != nil) != tc.wantErr {
				t.Fatal(result, err)
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatal("lost error identity", err)
			}
			if tc.wantErr || tc.empty {
				if result != (anylock.AcquireResult{}) {
					t.Fatal(result)
				}
			} else if !result.Acquired || result.Token != uint64(tc.token) {
				t.Fatal(result)
			}
		})
	}
}

func TestFencedValidationBeforeIO(t *testing.T) {
	d, err := New(noIODatabase(t), "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		ctx        context.Context
		key, owner string
		ttl        time.Duration
	}{
		{ctx: nil, key: "key", owner: "owner", ttl: time.Second},
		{ctx: ctx, key: "key", owner: "owner", ttl: time.Second},
		{ctx: context.Background(), key: "", owner: "owner", ttl: time.Second},
		{ctx: context.Background(), key: "key", owner: "owner", ttl: 0},
	} {
		if result, operationErr := d.TryAcquire(tc.ctx, tc.key, tc.owner, anylock.AcquireOptions{TTL: tc.ttl, Fencing: true}); operationErr == nil || result.Acquired {
			t.Fatal(result, operationErr)
		}
	}
}
