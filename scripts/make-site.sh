#!/bin/bash

SOCCO=true sbt ++2.12.6 examples/clean examples/compile
sbt ghpagesPushSite
