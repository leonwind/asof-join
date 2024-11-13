# Usage

Add the following to your Python code:

```python
from benchmark_plotter import style, texify, colors

texify.latexify(5, 1.8)
style.set_custom_style()
```

Do the rest normal, e.g. `plt.plot()`.

Generally, setting the tight layout for plots is recommended if they are inserted in LaTeX:

```python
plt.tight_layout()
```

For directly saving and removing the whitespace from figures:

```python
plt.savefig(path, dpi=400)
os.system(f"pdfcrop {path} {path}")
```
