package com.spotify.featran.java

import com.spotify.featran.FeatureExtractor

import java.util.{List => JList}

// java does not support higher-kind types
// specialize FeatureExtractor for java.util.List in the scala code
private class JListFeatureExtractor[T](other: FeatureExtractor[JList, T])
    extends FeatureExtractor[JList, T](other)(JavaOps.jListCollectionType)
