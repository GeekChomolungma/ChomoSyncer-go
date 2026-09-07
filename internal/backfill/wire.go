package backfill

import (
	"github.com/HarvestStars/chomosyncer-go/internal/chwriter"
	"github.com/HarvestStars/chomosyncer-go/internal/rediswin"
)

// Compile-time checks that the production types satisfy the injected interfaces.
var (
	_ KlineFetcher    = (*BinanceFetcher)(nil)
	_ KlineStore      = (*CHStore)(nil)
	_ ArchiveWriter   = (*chwriter.BatchWriter)(nil)
	_ WindowRebuilder = (*rediswin.Writer)(nil)
)
