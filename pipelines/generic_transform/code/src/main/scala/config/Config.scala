package config

import config.ConfigStore._
import pureconfig._
import io.prophecy.libs._

case class Config(
  fabricName:    String,
  SOURCE_PATH:   String,
  TRANSFORM_SQL: String,
  PLACE_HOLDER:  String,
  TARGET_PATH:   String
) extends ConfigBase
