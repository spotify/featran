#!/bin/bash

SOCCO=true sbt ++2.12.4 examples/clean examples/compile
sbt ghpagesPushSite
