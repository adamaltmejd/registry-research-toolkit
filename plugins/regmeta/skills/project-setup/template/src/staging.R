# The staging writer: every file the schema declares, exactly its columns, each cast
# to its declared type. A file with no table is written with zero rows; a declared
# column the table lacks is null; a column the schema does not declare is dropped.
CASTS <- list(integer = as.integer, double = as.double, string = as.character,
              boolean = as.logical, date = as.Date)

write_staging <- function(tables, schema, dir) {
  unlink(dir, recursive = TRUE)
  dir.create(dir, recursive = TRUE)
  for (file in jsonlite::read_json(schema)$files) {
    table <- tables[[file$name]]
    n <- if (is.null(table)) 0L else nrow(table)
    out <- lapply(file$columns, function(col) {
      x <- if (col$name %in% names(table)) table[[col$name]] else rep(NA, n)
      CASTS[[col$type]](x)
    })
    names(out) <- vapply(file$columns, function(col) col$name, "")
    arrow::write_parquet(as.data.frame(out), file.path(dir, file$name))
  }
}
