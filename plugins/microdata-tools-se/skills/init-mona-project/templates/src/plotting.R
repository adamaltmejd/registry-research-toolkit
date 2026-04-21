# Plotting helpers: device selection, save, and theme
#
# MONA may not have all graphics packages installed. The device selection
# logic falls back gracefully: svglite > builtin svg, ragg > builtin png.
# Run manage_packages.R on MONA to install the preferred packages.

#' Select the best available graphics device for a file extension
#'
#' Returns a device function suitable for ggplot2::ggsave(). Falls back
#' to built-in devices when optional packages (svglite, ragg) are missing.
#'
#' @param ext File extension without dot (e.g. "svg", "png", "pdf").
#' @return A device function, or NULL if the extension is not supported.
plot_device_for_extension <- function(ext) {
  if (ext == "pdf") {
    return(grDevices::cairo_pdf)
  }

  if (ext == "svg") {
    if (requireNamespace("svglite", quietly = TRUE)) {
      return(svglite::svglite)
    }
    return(NULL)
  }

  if (ext == "png") {
    if (requireNamespace("ragg", quietly = TRUE)) {
      return(ragg::agg_png)
    }
    return(grDevices::png)
  }

  NULL
}

#' Save a ggplot to a file using the best available device
#'
#' Default dimensions are 10 x 5.625 inches (16:9 aspect ratio), which
#' works well for presentations and papers. Override as needed.
#'
#' @param .plot A ggplot object.
#' @param fn Output file path. The extension determines the device.
#' @param width Width in inches (default: 10).
#' @param height Height in inches (default: 5.625, giving 16:9 ratio).
#' @param ... Additional arguments passed to ggplot2::ggsave.
#' @return The file path (for use as a targets file target).
save_plot <- function(.plot, fn, width = 10, height = 5.625, ...) {
  dev <- plot_device_for_extension(tools::file_ext(fn))
  if (is.null(dev)) {
    stop(
      sprintf("No graphics device available for `%s`.", tools::file_ext(fn)),
      call. = FALSE
    )
  }

  ggplot2::ggsave(
    filename = fn,
    plot = .plot,
    width = width,
    height = height,
    device = dev,
    ...
  )

  if (!file.exists(fn)) {
    stop(sprintf("Plot device did not create `%s`.", fn), call. = FALSE)
  }

  fn
}

#' Set the default ggplot2 theme for the project
#'
#' Called at the start of plot targets (via tar_save_plot) to ensure
#' consistent styling. Modify this function to change the project-wide
#' plot appearance.
plot_theme <- function() {
  ggplot2::theme_set(
    ggplot2::theme_light(base_size = 10, base_family = "sans")
  )
}
