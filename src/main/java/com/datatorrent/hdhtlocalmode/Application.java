/**
 * Put your copyright and license info here.
 */
package com.datatorrent.hdhtlocalmode;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build

    RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
    HDHTTestOperator hdhtOperator = dag.addOperator("HDHT Operator", HDHTTestOperator.class);

    TFileImpl storeFile = new TFileImpl.DTFileImpl();
    storeFile.setBasePath("hdhtlocaltest");

    hdhtOperator.setFileStore(storeFile);
    hdhtOperator.setFlushIntervalCount(1);
    hdhtOperator.setFlushSize(0);

    dag.addStream("HDHTStream", randomGenerator.out, hdhtOperator.input);
  }
}
