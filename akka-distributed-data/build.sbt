import akka.{ AkkaBuild, Dependencies, Formatting, MultiNode, Unidoc, OSGi }

AkkaBuild.defaultSettings
AkkaBuild.experimentalSettings
Formatting.formatSettings
OSGi.distributedData
Dependencies.distributedData

enablePlugins(MultiNodeScalaTest)

version := "2.4-DDATA-20161122-1"
