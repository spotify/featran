#!/bin/bash

SOCCO=true sbt ++2.12.4 examples/compile
sbt ghpagesPushSite
