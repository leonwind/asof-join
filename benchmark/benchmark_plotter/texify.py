"""This module defines the LaTex related settings passed to matplotlib."""
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np


def latexify(fig_width=None, fig_height=None, columns=1, width_heigth_ratio=None, custom_params=None):
    """Set up matplotlib's RC params for LaTeX plotting.
    Call this before plotting a figure.

    Parameters
    ----------
    width_heigth_ratio: float, optional
    fig_width : float, optional, inches
    fig_height : float,  optional, inches
    columns : {1, 2}
    """

    # code adapted from http://www.scipy.org/Cookbook/Matplotlib/LaTeX_Examples

    # Width and max height in inches for IEEE journals taken from
    # computer.org/cms/Computer.org/Journal%20templates/transactions_art_guide.pdf

    assert (columns in [1, 2])

    if fig_width is None:
        fig_width = 3.39 if columns == 1 else 6.9  # width in inches

    if fig_height is None:
        golden_mean = (np.sqrt(5) - 1.0) / 2.0  # Aesthetic ratio
        fig_height = fig_width * golden_mean  # height in inches
    print(fig_height)

    if width_heigth_ratio is not None:
        fig_height = fig_width / width_heigth_ratio

    max_height_inches = 32.0
    if fig_height > max_height_inches:
        print('WARNING: fig_height too large:' + fig_height +
              'so will reduce to' + max_height_inches + 'inches.')
        fig_height = max_height_inches

    print(f'fig width={fig_width}, fig height={fig_height}')
    #mpl.use('pgf')
    params = {
        'pgf.texsystem': 'xelatex',
        #'backend': 'ps',
        'pgf.rcfonts': False,
        'figure.labelsize': 8,
        'figure.titlesize': 9,
        'axes.labelsize': 8,  # fontsize for x and y labels
        'axes.titlesize': 8,
        'font.size': 8,
        'legend.fontsize': 8,
        'legend.handlelength': 1,
        'legend.handletextpad': 0.5,
        'legend.labelspacing': 0.1,  # was 0.1
        'legend.columnspacing': 1.5,
        'legend.borderpad': 0.3,
        'xtick.labelsize': 8,
        'ytick.labelsize': 8,
        'axes.labelpad': 3,
        'axes.titlepad': 3,
        'text.usetex': True,
        'font.family': 'serif',
        'grid.linewidth': 0.5,
        'figure.figsize': [fig_width, fig_height],
        #'pgf.preamble': [r'\usepackage{xcolor}', r'\usepackage{xfrac}', r'\usepackage{tikz}', r'\usepackage{amssymb}']
    }

    if custom_params is not None:
        for k, v in custom_params.items():
            params[k] = v
    plt.rcParams.update(params)


def format_axes(ax):
    spine_color = 'black'
    for spine in ['top', 'right']:
        ax.spines[spine].set_visible(False)

    for spine in ['left', 'bottom']:
        ax.spines[spine].set_color(spine_color)
        ax.spines[spine].set_linewidth(0.5)

    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')

    for axis in [ax.xaxis, ax.yaxis]:
        axis.set_tick_params(direction='out', color=spine_color)

    return ax


def bar_axes(ax):
    ax.set_axisbelow(True)


def cm2inch(value):
    return value / 2.54
