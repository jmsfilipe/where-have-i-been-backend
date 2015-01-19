def smooth(gpx, remove_extremes, how_much_to_smooth, min_sameness_distance, min_sameness_interval):
    from . import median as mod_median

    return mod_median.smooth(gpx, remove_extremes, how_much_to_smooth, min_sameness_distance, min_sameness_interval)
