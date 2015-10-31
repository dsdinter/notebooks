# Databricks notebook source exported at Sat, 31 Oct 2015 13:24:28 UTC
data <- read.df(sqlContext, "hdfs:/data/tweetsdump", source = "json")

# COMMAND ----------

registerTempTable(data, "tweets")

# COMMAND ----------

# MAGIC %sql select * from tweets limit 5

# COMMAND ----------

library(magrittr)

# COMMAND ----------

# MAGIC %md We are only interested in text features. So we select that column and cache the DataFrame

# COMMAND ----------

textData <- data %>%
  selectExpr("lower(text) as text") %>%
  withColumn("isHappy", like(.$text, "%:-)%")) %>%
  withColumn("isSad", like(.$text, "%:-(%")) %>%  
  cache

# COMMAND ----------

nrow(textData)

# COMMAND ----------

# MAGIC %md Counting number of positive and negative examples

# COMMAND ----------

emotions <- textData  %>%
  filter(.$isHappy != .$isSad) %>%
  group_by("isHappy", "isSad") %>%
  count

# COMMAND ----------

# MAGIC %md We seem to have many more positive examples

# COMMAND ----------

display(emotions)

# COMMAND ----------

# MAGIC %md We sample positive examples to get a balanced training set

# COMMAND ----------

happy <- textData %>%
  filter(.$isHappy) %>%
  sample(F, 0.1)

# COMMAND ----------

sad <- textData %>%
  filter(.$isSad)

# COMMAND ----------

sad %>% rbind(happy) %>% registerTempTable("tweetData")