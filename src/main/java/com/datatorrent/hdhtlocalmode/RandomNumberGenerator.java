/**
 * Put your copyright and license info here.
 */
package com.datatorrent.hdhtlocalmode;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple operator that emits random number.
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator
{
  private boolean emitted = false;

  public final transient DefaultOutputPort<Pair> out = new DefaultOutputPort<Pair>();

  @Override
  public void emitTuples()
  {
    if (!emitted) {
      out.emit(new Pair(1, 1));

      emitted = true;
    }
  }
}
