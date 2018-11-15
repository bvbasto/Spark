// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # ** HTML, Javascript, D3, and SVG **
// MAGIC You can view HTML code, including Javascript, D3, SVG and more in Python, Scala, and R notebooks, with the `displayHTML` method.
// MAGIC 
// MAGIC However if you link to anything across the web, us `https://` instead of `http://` when linking to external resources. If you don't, your graphics, images, or Javascript may not render correctly due to mixed content errors.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Call the **displayHTML** function to view HTML code in your notebook.

// COMMAND ----------

displayHTML("<h3>You can view HTML code in notebooks.</h3>")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Display **SVG visualizations** with **displayHTML**.

// COMMAND ----------

displayHTML("""<svg width="100" height="100">
   <circle cx="50" cy="50" r="40" stroke="green" stroke-width="4" fill="yellow" />
   Sorry, your browser does not support inline SVG.
</svg>""")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Display Javascript visualizations with `displayHTML`.
// MAGIC If you have custom Javascript libraries to include, see the [FileStore example](https://docs.azuredatabricks.net/user-guide/advanced/filestore.html#use-a-javascript-library).

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Display **D3 visualizations** with **displayHTML**.
// MAGIC **Note:** You can program part of the HTML code for the D3 visualizations from an RDD.
// MAGIC 
// MAGIC Find out more about D3 here: http://d3js.org/.

// COMMAND ----------

// Change these colors to your favorites to change the D3 visualization.
val colorsRDD = sc.parallelize(Array((197,27,125), (222,119,174), (241,182,218), (253,244,239), (247,247,247), (230,245,208), (184,225,134), (127,188,65), (77,146,33)))
val colors = colorsRDD.collect()

// COMMAND ----------

displayHTML(s"""
<!DOCTYPE html>
<meta charset="utf-8">
<style>

path {
  fill: yellow;
  stroke: #000;
}

circle {
  fill: #fff;
  stroke: #000;
  pointer-events: none;
}

.PiYG .q0-9{fill:rgb${colors(0)}}
.PiYG .q1-9{fill:rgb${colors(1)}}
.PiYG .q2-9{fill:rgb${colors(2)}}
.PiYG .q3-9{fill:rgb${colors(3)}}
.PiYG .q4-9{fill:rgb${colors(4)}}
.PiYG .q5-9{fill:rgb${colors(5)}}
.PiYG .q6-9{fill:rgb${colors(6)}}
.PiYG .q7-9{fill:rgb${colors(7)}}
.PiYG .q8-9{fill:rgb${colors(8)}}

</style>
<body>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.6/d3.min.js"></script>
<script>

var width = 960,
    height = 500;

var vertices = d3.range(100).map(function(d) {
  return [Math.random() * width, Math.random() * height];
});

var svg = d3.select("body").append("svg")
    .attr("width", width)
    .attr("height", height)
    .attr("class", "PiYG")
    .on("mousemove", function() { vertices[0] = d3.mouse(this); redraw(); });

var path = svg.append("g").selectAll("path");

svg.selectAll("circle")
    .data(vertices.slice(1))
  .enter().append("circle")
    .attr("transform", function(d) { return "translate(" + d + ")"; })
    .attr("r", 2);

redraw();

function redraw() {
  path = path.data(d3.geom.delaunay(vertices).map(function(d) { return "M" + d.join("L") + "Z"; }), String);
  path.exit().remove();
  path.enter().append("path").attr("class", function(d, i) { return "q" + (i % 9) + "-9"; }).attr("d", String);
}

</script>
  """)

// COMMAND ----------

