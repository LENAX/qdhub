package impl

import (
	"context"
	"testing"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	domainSync "qdhub/internal/domain/sync"
)

func TestStrategyToParamDependencies_UsesIndexBasicForIndexAPI(t *testing.T) {
	strategy := &metadata.APISyncStrategy{
		PreferredParam: metadata.SyncParamTsCode,
	}

	deps := strategyToParamDependencies("index_daily", strategy)
	if len(deps) != 1 {
		t.Fatalf("expected 1 dependency, got %d", len(deps))
	}
	if deps[0].SourceAPI != "index_basic" {
		t.Fatalf("expected source api index_basic, got %s", deps[0].SourceAPI)
	}
	if deps[0].ParamName != "ts_code" || deps[0].SourceField != "ts_code" || !deps[0].IsList {
		t.Fatalf("unexpected dependency: %+v", deps[0])
	}
}

func TestStrategyToParamDependencies_UsesStockBasicForStockAPI(t *testing.T) {
	strategy := &metadata.APISyncStrategy{
		PreferredParam: metadata.SyncParamTsCode,
	}

	deps := strategyToParamDependencies("daily", strategy)
	if len(deps) != 1 {
		t.Fatalf("expected 1 dependency, got %d", len(deps))
	}
	if deps[0].SourceAPI != "stock_basic" {
		t.Fatalf("expected source api stock_basic, got %s", deps[0].SourceAPI)
	}
}

func TestStrategyToParamDependencies_IndexCodeUsesIndexBasic(t *testing.T) {
	strategy := &metadata.APISyncStrategy{
		PreferredParam: metadata.SyncParamIndexCode,
	}

	deps := strategyToParamDependencies("index_weight", strategy)
	if len(deps) != 1 {
		t.Fatalf("expected 1 dependency, got %d", len(deps))
	}
	if deps[0].ParamName != "index_code" {
		t.Fatalf("expected param_name index_code, got %s", deps[0].ParamName)
	}
	if deps[0].SourceAPI != "index_basic" || deps[0].SourceField != "ts_code" || !deps[0].IsList {
		t.Fatalf("unexpected dependency: %+v", deps[0])
	}
}

func TestSupplementDependenciesFromStrategies_IndexDailyEndToEnd(t *testing.T) {
	dataSourceID := shared.NewID()
	repo := &fakeMetadataRepository{
		strategies: []*metadata.APISyncStrategy{
			{
				ID:             shared.NewID(),
				DataSourceID:   dataSourceID,
				APIName:        "index_daily",
				PreferredParam: metadata.SyncParamTsCode,
			},
		},
	}
	svc := &SyncApplicationServiceImpl{
		metadataRepo:       repo,
		dependencyResolver: domainSync.NewDependencyResolver(),
	}

	allAPIs := []*metadata.APIMetadata{
		{Name: "index_basic"},
		{Name: "index_daily"},
	}
	allAPIDependencies := map[string][]domainSync.ParamDependency{
		"index_basic": {},
		"index_daily": {},
	}

	svc.supplementDependenciesFromStrategies(context.Background(), dataSourceID, allAPIs, allAPIDependencies)

	graph, _, err := svc.dependencyResolver.Resolve([]string{"index_daily"}, allAPIDependencies)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}

	cfg := graph.TaskConfigs["index_daily"]
	if cfg == nil {
		t.Fatal("expected task config for index_daily")
	}
	if len(cfg.Dependencies) != 1 || cfg.Dependencies[0] != "FetchIndexBasic" {
		t.Fatalf("expected dependency FetchIndexBasic, got %+v", cfg.Dependencies)
	}
	if len(cfg.ParamMappings) != 1 || cfg.ParamMappings[0].SourceTask != "FetchIndexBasic" {
		t.Fatalf("expected source task FetchIndexBasic, got mappings %+v", cfg.ParamMappings)
	}
}

type fakeMetadataRepository struct {
	strategies []*metadata.APISyncStrategy
}

func (f *fakeMetadataRepository) SaveCategories(ctx context.Context, categories []metadata.APICategory) error {
	return nil
}
func (f *fakeMetadataRepository) DeleteCategoriesByDataSource(ctx context.Context, dataSourceID shared.ID) error {
	return nil
}
func (f *fakeMetadataRepository) SaveAPIMetadata(ctx context.Context, meta *metadata.APIMetadata) error {
	return nil
}
func (f *fakeMetadataRepository) SaveAPIMetadataBatch(ctx context.Context, metas []metadata.APIMetadata) error {
	return nil
}
func (f *fakeMetadataRepository) DeleteAPIMetadata(ctx context.Context, id shared.ID) error {
	return nil
}
func (f *fakeMetadataRepository) DeleteAPIMetadataByDataSource(ctx context.Context, dataSourceID shared.ID) error {
	return nil
}
func (f *fakeMetadataRepository) GetDataSource(ctx context.Context, id shared.ID) (*metadata.DataSource, error) {
	return nil, nil
}
func (f *fakeMetadataRepository) GetDataSourceByName(ctx context.Context, name string) (*metadata.DataSource, error) {
	return nil, nil
}
func (f *fakeMetadataRepository) GetToken(ctx context.Context, dataSourceID shared.ID) (*metadata.Token, error) {
	return nil, nil
}
func (f *fakeMetadataRepository) GetAPIMetadata(ctx context.Context, id shared.ID) (*metadata.APIMetadata, error) {
	return nil, nil
}
func (f *fakeMetadataRepository) ListCategoriesByDataSource(ctx context.Context, dataSourceID shared.ID) ([]metadata.APICategory, error) {
	return nil, nil
}
func (f *fakeMetadataRepository) ListCategoriesByDataSourceWithAPIs(ctx context.Context, dataSourceID shared.ID) ([]metadata.APICategory, error) {
	return nil, nil
}
func (f *fakeMetadataRepository) ListAPIMetadataByDataSource(ctx context.Context, dataSourceID shared.ID) ([]metadata.APIMetadata, error) {
	return nil, nil
}
func (f *fakeMetadataRepository) ListAPIMetadataByDataSourcePaginated(ctx context.Context, dataSourceID shared.ID, idFilter *shared.ID, nameFilter string, categoryIDFilter *shared.ID, page, pageSize int) ([]metadata.APIMetadata, int64, error) {
	return nil, 0, nil
}
func (f *fakeMetadataRepository) SaveAPISyncStrategy(ctx context.Context, strategy *metadata.APISyncStrategy) error {
	return nil
}
func (f *fakeMetadataRepository) SaveAPISyncStrategyBatch(ctx context.Context, strategies []*metadata.APISyncStrategy) error {
	return nil
}
func (f *fakeMetadataRepository) GetAPISyncStrategyByID(ctx context.Context, id shared.ID) (*metadata.APISyncStrategy, error) {
	return nil, nil
}
func (f *fakeMetadataRepository) GetAPISyncStrategyByAPIName(ctx context.Context, dataSourceID shared.ID, apiName string) (*metadata.APISyncStrategy, error) {
	return nil, nil
}
func (f *fakeMetadataRepository) ListAPISyncStrategiesByDataSource(ctx context.Context, dataSourceID shared.ID) ([]*metadata.APISyncStrategy, error) {
	var out []*metadata.APISyncStrategy
	for _, s := range f.strategies {
		if s.DataSourceID == dataSourceID {
			out = append(out, s)
		}
	}
	return out, nil
}
func (f *fakeMetadataRepository) ListAPISyncStrategiesByAPINames(ctx context.Context, dataSourceID shared.ID, apiNames []string) ([]*metadata.APISyncStrategy, error) {
	nameSet := make(map[string]bool, len(apiNames))
	for _, n := range apiNames {
		nameSet[n] = true
	}
	var out []*metadata.APISyncStrategy
	for _, s := range f.strategies {
		if s.DataSourceID == dataSourceID && nameSet[s.APIName] {
			out = append(out, s)
		}
	}
	return out, nil
}
func (f *fakeMetadataRepository) DeleteAPISyncStrategy(ctx context.Context, id shared.ID) error {
	return nil
}
func (f *fakeMetadataRepository) DeleteAPISyncStrategiesByDataSource(ctx context.Context, dataSourceID shared.ID) error {
	return nil
}
