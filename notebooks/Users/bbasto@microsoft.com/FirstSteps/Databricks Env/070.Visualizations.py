# Databricks notebook source
dataPath = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"
diamonds = spark.read.format("csv").option("header","true").option("inferSchema", "true").load(dataPath) # Read diamonds dataset and crate DataFrame
dm = diamonds.groupBy("color").avg("price") # Group by color
display(dm)

# COMMAND ----------

# MAGIC %r
# MAGIC fit <- lm(Petal.Length ~., data = iris)
# MAGIC layout(matrix(c(1,2,3,4),2,2)) # optional 4 graphs/page
# MAGIC plot(fit)

# COMMAND ----------

# MAGIC %r
# MAGIC library(ggplot2)
# MAGIC ggplot(diamonds, aes(carat, price, color = color, group = 1)) + geom_point(alpha = 0.3) + stat_smooth()

# COMMAND ----------

# MAGIC %r
# MAGIC library(lattice)
# MAGIC xyplot(price ~ carat | cut, diamonds, scales = list(log = TRUE), type = c("p", "g", "smooth"), ylab = "Log price")

# COMMAND ----------

# MAGIC %r
# MAGIC install.packages("DandEFA", repos = "http://cran.us.r-project.org")
# MAGIC library(DandEFA)
# MAGIC data(timss2011)
# MAGIC timss2011 <- na.omit(timss2011)
# MAGIC dandpal <- rev(rainbow(100, start = 0, end = 0.2))
# MAGIC facl <- factload(timss2011,nfac=5,method="prax",cormeth="spearman")
# MAGIC dandelion(facl,bound=0,mcex=c(1,1.2),palet=dandpal)
# MAGIC facl <- factload(timss2011,nfac=8,method="mle",cormeth="pearson")
# MAGIC dandelion(facl,bound=0,mcex=c(1,1.2),palet=dandpal)

# COMMAND ----------

