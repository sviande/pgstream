// SPDX-License-Identifier: Apache-2.0

package ddlrewrite

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddsOnlyExcludedConstraints(t *testing.T) {
	t.Parallel()

	const prismaForeignKey = `-- AddForeignKey
ALTER TABLE "HomeChargingVehicle" ADD CONSTRAINT "HomeChargingVehicle_companyId_fkey" FOREIGN KEY ("companyId") REFERENCES "Company"("id") ON DELETE CASCADE ON UPDATE CASCADE`
	// 20230217103743_piana_loan_add_dates
	const severalChecks = `ALTER TABLE "PianaLoan"
ADD CONSTRAINT "endDateGTEstartDate" CHECK ("endDate" >= "startDate"),
ADD CONSTRAINT "invoiceDateGTEendDate" CHECK ("invoiceDate" >= "endDate"),
ADD CONSTRAINT "capitalPaymentDateGTEinvoiceDate" CHECK ("capitalPaymentDate" >= "invoiceDate"),
ADD CONSTRAINT "interestPaymentDateGTEinvoiceDate" CHECK ("interestPaymentDate" >= "invoiceDate")`
	// 20250923135018_apply_fee_schema_to_per_units_etoll_fees
	const severalForeignKeys = `-- Step 4: Add foreign key constraints
ALTER TABLE
  "ElectronicTollSubscription"
ADD
  CONSTRAINT "ElectronicTollSubscription_badgesCreationFeeId_fkey" FOREIGN KEY ("badgesCreationFeeId") REFERENCES "ElectronicTollSubscriptionFee" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
ADD
  CONSTRAINT "ElectronicTollSubscription_badgesSubscriptionFeeFrWithoutParkingId_fkey" FOREIGN KEY ("badgesSubscriptionFeeFrWithoutParkingId") REFERENCES "ElectronicTollSubscriptionFee" ("id") ON DELETE CASCADE ON UPDATE CASCADE`

	tests := []struct {
		name        string
		stmt        string
		foreignKeys bool
		checks      bool
		want        bool
	}{
		{name: "prisma foreign key", stmt: prismaForeignKey, foreignKeys: true, want: true},
		{name: "foreign key, foreign keys kept", stmt: prismaForeignKey, checks: true, want: false},
		{name: "foreign key, nothing excluded", stmt: prismaForeignKey, want: false},
		{name: "several foreign keys", stmt: severalForeignKeys, foreignKeys: true, want: true},
		{name: "several checks", stmt: severalChecks, checks: true, want: true},
		{name: "several checks, checks kept", stmt: severalChecks, foreignKeys: true, want: false},
		{
			name:        "unnamed foreign key",
			stmt:        `ALTER TABLE "Card" ADD FOREIGN KEY ("accountId") REFERENCES "Account"("id")`,
			foreignKeys: true,
			want:        true,
		},
		{
			name:   "unnamed check",
			stmt:   `alter table "Card" add check ("limit" > 0)`,
			checks: true,
			want:   true,
		},
		{
			name:   "check with a comma, parentheses and quotes in a literal",
			stmt:   `ALTER TABLE "Card" ADD CONSTRAINT "c" CHECK ("status" IN ('a,b', 'c)''d') AND ("x" > 0)) NOT VALID`,
			checks: true,
			want:   true,
		},
		{
			name:        "if exists, only, schema-qualified",
			stmt:        `ALTER TABLE IF EXISTS ONLY public."piana_Card" ADD CONSTRAINT "Card_accountId_fkey" FOREIGN KEY ("accountId") REFERENCES public."piana_Account"("id")`,
			foreignKeys: true,
			want:        true,
		},
		{
			name:        "foreign key and check, both excluded",
			stmt:        `ALTER TABLE "Card" ADD CONSTRAINT "f" FOREIGN KEY ("a") REFERENCES "A"("id"), ADD CONSTRAINT "c" CHECK ("a" <> '')`,
			foreignKeys: true,
			checks:      true,
			want:        true,
		},
		{
			name:        "foreign key and check, only foreign keys excluded",
			stmt:        `ALTER TABLE "Card" ADD CONSTRAINT "f" FOREIGN KEY ("a") REFERENCES "A"("id"), ADD CONSTRAINT "c" CHECK ("a" <> '')`,
			foreignKeys: true,
			want:        false,
		},
		{
			name:        "foreign key with another action",
			stmt:        `ALTER TABLE "Card" ADD COLUMN "a" TEXT, ADD CONSTRAINT "f" FOREIGN KEY ("a") REFERENCES "A"("id")`,
			foreignKeys: true,
			checks:      true,
			want:        false,
		},
		{
			name:        "column with an inline reference",
			stmt:        `ALTER TABLE "Card" ADD COLUMN "a" TEXT REFERENCES "A"("id")`,
			foreignKeys: true,
			want:        false,
		},
		{
			name:        "unique constraint",
			stmt:        `ALTER TABLE "Card" ADD CONSTRAINT "Card_a_key" UNIQUE ("a")`,
			foreignKeys: true,
			checks:      true,
			want:        false,
		},
		{
			name:        "primary key",
			stmt:        `ALTER TABLE "Card" ADD CONSTRAINT "Card_pkey" PRIMARY KEY ("id")`,
			foreignKeys: true,
			checks:      true,
			want:        false,
		},
		{
			name:        "column named check",
			stmt:        `ALTER TABLE "Card" ADD "check" BOOLEAN`,
			foreignKeys: true,
			checks:      true,
			want:        false,
		},
		{
			name:        "drop constraint",
			stmt:        `ALTER TABLE "Card" DROP CONSTRAINT "Card_accountId_fkey"`,
			foreignKeys: true,
			checks:      true,
			want:        false,
		},
		{
			name:        "create table with a foreign key",
			stmt:        `CREATE TABLE "A" ("id" TEXT, CONSTRAINT "f" FOREIGN KEY ("id") REFERENCES "B"("id"))`,
			foreignKeys: true,
			checks:      true,
			want:        false,
		},
		{
			name:        "comments around the keywords",
			stmt:        "/* fk */ ALTER TABLE \"Card\" -- table\n ADD /* named */ CONSTRAINT \"f\" FOREIGN KEY (\"a\") REFERENCES \"A\"(\"id\")",
			foreignKeys: true,
			want:        true,
		},
		{
			name:        "no action",
			stmt:        `ALTER TABLE "Card"`,
			foreignKeys: true,
			checks:      true,
			want:        false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, AddsOnlyExcludedConstraints(tc.stmt, tc.foreignKeys, tc.checks))
		})
	}
}

func TestSQLTokens(t *testing.T) {
	t.Parallel()

	got := tokenTexts(sqlTokens([]rune("alter table \"Ä b\" add check ('it''s; -- not a comment' <> $x$ a ) $x$) /* gone */ -- gone\n,")))
	require.Equal(t, []string{
		"ALTER", "TABLE", `"Ä b"`, "ADD", "CHECK", "(", `'it''s; -- not a comment'`, "<", ">", "$x$ a ) $x$", ")", ",",
	}, got)
}

// 20261027140000_add_credit_line_increase_requests
const creditLineIncreaseRequestTable = `-- CreateTable
CREATE TABLE "CreditLineIncreaseRequest" (
    "id" TEXT NOT NULL,
    "companyId" TEXT NOT NULL,
    "product" "ProductType" NOT NULL,
    "duration" "CreditLineIncreaseDuration" NOT NULL,
    "status" "CreditLineIncreaseRequestStatus" NOT NULL DEFAULT 'Pending',
    "accountId" TEXT,
    "mobilityCustomerId" TEXT,
    "currentAmount" INTEGER NOT NULL,
    "consumedAmount" INTEGER NOT NULL,
    "periodStartAt" TIMESTAMP(3) NOT NULL,
    "periodEndAt" TIMESTAMP(3) NOT NULL,
    "requestedAmount" INTEGER NOT NULL,
    "comment" TEXT,
    "requestedById" TEXT,
    "reviewedByEmail" TEXT,
    "reviewStartedAt" TIMESTAMP(3),
    "grantedAmount" INTEGER,
    "decisionMessage" TEXT,
    "decidedByEmail" TEXT,
    "decidedAt" TIMESTAMP(3),
    "decisionStartedAt" TIMESTAMP(3),
    "restoreState" JSONB,
    "restoreAt" TIMESTAMP(3),
    "restoredAt" TIMESTAMP(3),
    "opsNotifiedAt" TIMESTAMP(3),
    "requesterNotifiedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "CreditLineIncreaseRequest_pkey" PRIMARY KEY ("id"),
    -- Amounts are in cents. The line and its consumption are a snapshot the server read when the
    -- request was made, never a figure the client sent.
    CONSTRAINT "CreditLineIncreaseRequest_amounts_check" CHECK (
        "currentAmount" >= 0
        AND "consumedAmount" >= 0
        AND "requestedAmount" > "currentAmount"
        AND ("grantedAmount" IS NULL OR "grantedAmount" > 0)
    ),
    CONSTRAINT "CreditLineIncreaseRequest_period_check" CHECK ("periodEndAt" > "periodStartAt"),
    -- Only the products that have a credit line, each pointing at the entity that holds its line and
    -- only at it.
    CONSTRAINT "CreditLineIncreaseRequest_product_target_check" CHECK (
        ("product" = 'Card' AND "accountId" IS NOT NULL AND "mobilityCustomerId" IS NULL)
        OR ("product" = 'Mobility' AND "mobilityCustomerId" IS NOT NULL AND "accountId" IS NULL)
    ),
    -- A decision is dated and signed, an approval says how much was granted, and only a request someone
    -- took can be decided.
    CONSTRAINT "CreditLineIncreaseRequest_decision_check" CHECK (
        ("status" IN ('Approved', 'Declined')) = ("decidedAt" IS NOT NULL AND "decidedByEmail" IS NOT NULL)
        AND ("status" = 'Approved') = ("grantedAmount" IS NOT NULL)
        AND ("status" = 'Pending' OR "reviewedByEmail" IS NOT NULL)
    ),
    -- Only an approved temporary increase is ever restored.
    CONSTRAINT "CreditLineIncreaseRequest_restore_check" CHECK (
        "restoreAt" IS NULL OR ("status" = 'Approved' AND "duration" = 'Temporary')
    )
);`

func TestStripExcludedTableConstraints(t *testing.T) {
	t.Parallel()

	const tableForeignKey = `CREATE TABLE "Card" ("id" TEXT NOT NULL, "accountId" TEXT, CONSTRAINT "Card_pkey" PRIMARY KEY ("id"), CONSTRAINT "Card_accountId_fkey" FOREIGN KEY ("accountId") REFERENCES "Account"("id") ON DELETE CASCADE);`

	// the CHECK constraints follow the primary key, up to the closing parenthesis
	checksFrom := strings.Index(creditLineIncreaseRequestTable, ",\n    -- Amounts are in cents")
	checksTo := strings.LastIndex(creditLineIncreaseRequestTable, ")")
	require.Positive(t, checksFrom)

	tests := []struct {
		name        string
		stmt        string
		foreignKeys bool
		checks      bool
		want        string
	}{
		{
			name:   "prisma table with commented checks",
			stmt:   creditLineIncreaseRequestTable,
			checks: true,
			want:   creditLineIncreaseRequestTable[:checksFrom] + creditLineIncreaseRequestTable[checksTo:],
		},
		{
			name:        "prisma table, checks kept",
			stmt:        creditLineIncreaseRequestTable,
			foreignKeys: true,
			want:        creditLineIncreaseRequestTable,
		},
		{
			name:        "table foreign key",
			stmt:        tableForeignKey,
			foreignKeys: true,
			want:        `CREATE TABLE "Card" ("id" TEXT NOT NULL, "accountId" TEXT, CONSTRAINT "Card_pkey" PRIMARY KEY ("id"));`,
		},
		{
			name:   "table foreign key, foreign keys kept",
			stmt:   tableForeignKey,
			checks: true,
			want:   tableForeignKey,
		},
		{
			name: "nothing excluded",
			stmt: tableForeignKey,
			want: tableForeignKey,
		},
		{
			name:        "first and unnamed constraints",
			stmt:        `create unlogged table if not exists public."Card" (check ("a" > 0), "a" INT, foreign key ("a") references "A"("id"), "b" INT) WITH (fillfactor = 70)`,
			foreignKeys: true,
			checks:      true,
			want:        `create unlogged table if not exists public."Card" ( "a" INT, "b" INT) WITH (fillfactor = 70)`,
		},
		{
			name:        "constraints in column definitions are left in place",
			stmt:        `CREATE TABLE "Card" ("id" TEXT PRIMARY KEY, "accountId" TEXT REFERENCES "Account"("id"), "limit" INT CHECK ("limit" > 0))`,
			foreignKeys: true,
			checks:      true,
			want:        `CREATE TABLE "Card" ("id" TEXT PRIMARY KEY, "accountId" TEXT REFERENCES "Account"("id"), "limit" INT CHECK ("limit" > 0))`,
		},
		{
			name:        "unique and exclusion constraints are kept",
			stmt:        `CREATE TABLE "Card" ("a" INT, CONSTRAINT "u" UNIQUE ("a"), EXCLUDE USING gist ("a" WITH =))`,
			foreignKeys: true,
			checks:      true,
			want:        `CREATE TABLE "Card" ("a" INT, CONSTRAINT "u" UNIQUE ("a"), EXCLUDE USING gist ("a" WITH =))`,
		},
		{
			name:        "create table as",
			stmt:        `CREATE TABLE "Copy" AS SELECT * FROM "Card" WHERE "a" IN (1, 2)`,
			foreignKeys: true,
			checks:      true,
			want:        `CREATE TABLE "Copy" AS SELECT * FROM "Card" WHERE "a" IN (1, 2)`,
		},
		{
			name:        "partition of",
			stmt:        `CREATE TABLE "Card_2026" PARTITION OF "Card" (CONSTRAINT "c" CHECK ("a" > 0)) FOR VALUES IN (2026)`,
			foreignKeys: true,
			checks:      true,
			want:        `CREATE TABLE "Card_2026" PARTITION OF "Card" (CONSTRAINT "c" CHECK ("a" > 0)) FOR VALUES IN (2026)`,
		},
		{
			name:        "alter table",
			stmt:        `ALTER TABLE "Card" ADD CONSTRAINT "c" CHECK ("a" > 0)`,
			foreignKeys: true,
			checks:      true,
			want:        `ALTER TABLE "Card" ADD CONSTRAINT "c" CHECK ("a" > 0)`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, StripExcludedTableConstraints(tc.stmt, tc.foreignKeys, tc.checks))
		})
	}
}
