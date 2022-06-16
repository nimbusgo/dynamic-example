package config

import config.ConfigStore._
import pureconfig._
import io.prophecy.libs._

case class Config(
  fabricName:  String,
  config:      String,
  SOURCE_PATH: String,
  TARGET_PATH: String
) extends ConfigBase
