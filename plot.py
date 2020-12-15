import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import write
from parameters import pwc_durations, kocs


def initialize(data=None, x_label=None, y_label=None, x_max=None, y_max=None, x_min=0, y_min=0, lbl=None):
    x_max = np.nanmax(data) if x_max is None else x_max
    y_max = np.nanmax(data) if y_max is None else y_max
    plt.ylim([y_min, y_max])
    plt.xlim([x_min, x_max])
    if all((x_label, y_label)):
        plt.xlabel(x_label, fontsize=12)
        plt.ylabel(y_label, fontsize=12)
    axis = plt.gca()
    if lbl is not None:
        axis.set_label(lbl)


def scenarios(in_scenarios, selection, region, class_name, class_num):
    x_label, y_label = 'Concentration (μg/L)', 'Percentile'

    initialize(in_scenarios.conc, x_label, y_label, y_max=101)
    for koc in kocs:
        for duration in pwc_durations:
            sample_set = in_scenarios[(in_scenarios.duration == duration) & (in_scenarios.koc == koc)]
            concs, pctiles = sample_set[['conc', '%ile']].values.T
            plt.scatter(concs, pctiles, s=1, label=duration)
            sample_selection = selection[(selection.duration == duration) & (selection.koc == koc)]
            selected_conc, selected_pct = sample_selection[['conc', '%ile']].values.T
            plt.scatter(selected_conc, selected_pct, s=50)
        write.plot(region, class_name, class_num, koc, 'combined', clear=True, legend=True, legend_title='Duration')
