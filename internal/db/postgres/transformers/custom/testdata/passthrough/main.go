// Passthrough external transformer used by the custom command tests to verify the
// framed interaction protocol end-to-end (handshake + sequenced envelopes). It accepts
// the "id" column and returns every record unchanged.
package main

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var def = toolkit.NewTransformerDefinition(
	"Passthrough",
	newPassthrough,
).SetDescription("protocol passthrough verifier").
	SetMode(&toolkit.DriverParams{
		Name:                 toolkit.JsonModeName,
		JsonDataFormat:       toolkit.JsonTextDataFormatName,
		JsonAttributesFormat: toolkit.JsonAttributesNamesFormatName,
	}).
	AddParameter(
		toolkit.MustNewParameterDefinition("column", "affected column").
			SetIsColumn(toolkit.NewColumnProperties().
				SetAffected(true).
				SetAllowedColumnTypes("int2", "int4"),
			).
			SetRequired(true),
	)

type passthrough struct{}

func newPassthrough(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (
	toolkit.Transformer, toolkit.ValidationWarnings, error,
) {
	return &passthrough{}, nil, nil
}

func (p *passthrough) Init(ctx context.Context) error { return nil }
func (p *passthrough) Done(ctx context.Context) error { return nil }
func (p *passthrough) Validate(ctx context.Context) (toolkit.ValidationWarnings, error) {
	return nil, nil
}
func (p *passthrough) Transform(ctx context.Context, r *toolkit.Record) error {
	return nil
}

func main() {
	cmd := toolkit.NewCmd(def)
	if err := cmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("")
	}
}
