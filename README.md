# mysqldump-loader

Load a MySQL dump file using LOAD DATA INFILE.

## Usage

    ./mysqldump-loader

### Flags

    $ ./mysqldump-loader --help
    Usage of ./mysqldump-loader:
      -concurrency int
            Maximum number of concurrent load operations. (default Number of available CPUs)
      -data-source-name string
            Data source name for MySQL server to load data into.
      -dump-file string
            MySQL dump file to load.
      -low-priority
            Use LOW_PRIORITY when loading data.
      -mysql-variable value
            MySQL variable (format: <name>=<value>)
      -replace-table
            Load data into a temporary table and replace the old table with it once load is complete.
      -verbose
            Verbose mode.

## Development

### Building

    go build
