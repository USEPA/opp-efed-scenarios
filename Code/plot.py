import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import write
from parameters import percentile_field, selection_percentiles
from utilities import report

def initialize(lbl=None, data=None, x_label=None, y_label=None, x_max=None, y_max=None, x_min=0, y_min=0):
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


def overlay_selected(selection, field, draw_lines=False, label_selected=True, selection_label=None):
    sel_concentrations, sel_percentiles, sel_targets = \
        selection.loc[selection.subject == field, ['concentration', 'percentile', 'target']].values.T
    for x, y in zip(sel_concentrations, sel_targets):
        if draw_lines:
            plt.axhline(y=y, ls="--", lw=0.5)
            plt.axvline(x=x, ls="--", lw=0.5)
        if label_selected:
            plt.text(0, y, int(y), fontsize=8)
            plt.text(x, 0, round(x, 1), fontsize=8)
    plt.scatter(sel_concentrations, sel_percentiles, s=50, label=selection_label)


def remove_outliers(table, fields, retain=True, cutoff=None):
    # Manage outliers for display
    if cutoff is None:  # z score method
        table['outlier'] = (table[fields].apply(stats.zscore).abs() > 6).any(axis=1)
    else:
        fields = [percentile_field.format(field) for field in fields]
        table['outlier'] = (table[fields] > cutoff).any(axis=1)
    table = table[~table.outlier]
    if not retain:
        del table['outlier']
    return table


def normalize(scenarios, fields, selection=None):
    for field in fields:
        pct_field = percentile_field.format(field)
        rank = (scenarios[pct_field] - 50.).abs().sort_values().index
        scenarios[field] /= scenarios.loc[rank].iloc[0][field]
        if selection is not None:
            conc_50 = selection[(selection.subject == field) & (selection.target == 50)]['concentration'].values[0]
            selection.loc[selection.subject == field, 'concentration'] /= conc_50
    return scenarios, selection


def percentiles(scenarios, fields, plot_outfile=None, selection=None, combined=True,
                labels=None, combined_label=None, individual_label=None, selection_label=None,
                normalize_50=False):
    x_label, y_label = 'Acute EDWC (μg/L)', 'Percentile'
    draw_lines = False
    label_selected = False

    scenarios = remove_outliers(scenarios, fields, cutoff=99.5)

    if normalize_50:
        scenarios, selection = normalize(scenarios, fields, selection)
        x_label = r'EEC / EEC50'

    outfiles = []
    if combined:
        initialize(combined_label, scenarios[fields], x_label, y_label, y_max=101)
    for i, field in enumerate(fields):
        concs, pctiles = scenarios[[field, percentile_field.format(field)]].values.T
        if combined:
            label = labels[i] if labels is not None else None
        else:
            label = labels
            initialize(individual_label, concs, x_label, y_label, y_max=101)
        plt.scatter(concs, pctiles, s=1, label=label)
        if selection is not None:
            overlay_selected(selection, field, draw_lines, label_selected, selection_label)
        if not combined and plot_outfile is not None:
            write.plot(plot_outfile.format(field), True)
            outfiles.append(plot_outfile.format(field))
    if combined and plot_outfile is not None:
        write.plot(plot_outfile.format("combined"), position='lower right')

    return outfiles


def ratios(ratio_table, out_path, cdl_name, field1, field2, vars1, vars2):
    print(ratio_table)
    vars1 = list(map(str, vars1))
    vars2 = list(map(str, vars2))
    x_label, y_label = 'Region', 'Concentration (μg/L)'
    regions = [''] + sorted(ratio_table.region.unique())
    region_labels = [r.lstrip("r") for r in regions]
    # TODO - clean this up going forward
    # Plot percentiles for each region
    for chart_type in ('dot', 'bar'):
        for var1, var2 in ratio_table[[field1, 'subject']].drop_duplicates().values:
            table = ratio_table[(ratio_table.subject == var2) & (ratio_table[field1] == var1)]
            if table.empty:
                continue
            initialize(data=table[selection_percentiles], x_label=x_label, y_label=y_label, x_max=len(regions))
            plt.xticks(range(len(regions)), region_labels, size='small')
            region_index = np.array([regions.index(r) for r in table.region])
            for pct in selection_percentiles[::-1]:
                if chart_type == 'dot':
                    plt.scatter(region_index, table[pct], s=10, label=pct)
                else:
                    plt.bar(region_index, table[pct], width=0.8, label=pct)
            plot_outfile = out_path.format(cdl_name, f'all-pct-{chart_type}', var2, var1)
            write.plot(plot_outfile, True, "Percentiles", position='upper left')

    pct = 90
    pct_field = f'r_{pct}/50'
    y_label = '90%ile / 50%ile'
    for chart_type in ('dot', 'bar'):
        # Plot 90/50 ratio for each region, with a series for each koc
        for var1 in vars1:
            initialize(initialize(data=ratio_table[pct_field], x_label=x_label, y_label=y_label, x_max=len(regions)))
            plt.xticks(range(len(regions)), region_labels, size='small')
            for var2 in vars2:
                table = ratio_table[(ratio_table.subject == var2) & (ratio_table[field1] == var1)]
                if table.empty:
                    continue
                try:
                    if chart_type == 'dot':
                        plt.scatter(region_index, table[pct_field], s=10, label=var2)
                    else:
                        plt.bar(region_index, table[pct_field], width=0.8, label=var2)
                except Exception as e:
                    report(f"Unable to make ratio plot for {var1}/{var2}")
            plot_outfile = out_path.format(cdl_name, f'90-50-{chart_type}', f'all-{field2}', var1)
            write.plot(plot_outfile, True, field2.capitalize(), position="upper left")