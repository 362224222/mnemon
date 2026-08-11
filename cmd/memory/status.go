package memory

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show memory statistics",
	Long:  "Display aggregate statistics about stored insights and graph edges.",
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := openDB()
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer db.Close()

		stats, err := db.GetStats()
		if err != nil {
			return fmt.Errorf("get stats: %w", err)
		}

		// Get file size
		var fileSize int64
		if fi, err := os.Stat(db.Path()); err == nil {
			fileSize = fi.Size()
		}

		output := map[string]interface{}{
			"total_insights":   stats.Total,
			"deleted_insights": stats.DeletedCount,
			"by_category":      stats.ByCategory,
			"edge_count":       stats.EdgeCount,
			"top_entities":     stats.TopEntities,
			"oplog_count":      stats.OplogCount,
			"db_path":          db.Path(),
			"db_size_bytes":    fileSize,
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(output)
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)
}
