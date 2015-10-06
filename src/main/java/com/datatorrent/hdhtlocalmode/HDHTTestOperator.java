/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.hdhtlocalmode;

import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;
import java.io.IOException;
import java.io.Serializable;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.LoggerFactory;

public class HDHTTestOperator extends AbstractSinglePortHDHTWriter<Pair>
{
  @Override
  public void processEvent(Pair val)
  {
    byte[] value = load(0L, new Slice(GPOUtils.serializeInt(val.key)));

    LOG.debug("Got Pair {}", val);

    if(value != null) {
      val.value += GPOUtils.deserializeInt(value);
    }

    try {
      this.put(0L, new Slice(getCodec().getKeyBytes(val)), getCodec().getValueBytes(val));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    LOG.info("=======NEW VALUE:::: {}", val.value);
  }

  public byte[] load(long bucketID, Slice keySlice)
  {
    byte[] val = getUncommitted(bucketID, keySlice);

    if (val == null) {
      try {
        val = get(bucketID, keySlice);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    return val;
  }

  @Override
  protected HDHTCodec<Pair> getCodec()
  {
    return new MyCodec();
  }

  public static class MyCodec implements HDHTCodec<Pair>, Serializable
  {
    private static final long serialVersionUID = 201510020616L;

    @Override
    public byte[] getKeyBytes(Pair event)
    {
      return GPOUtils.serializeInt(event.key);
    }

    @Override
    public byte[] getValueBytes(Pair event)
    {
      return GPOUtils.serializeInt(event.value);
    }

    @Override
    public Pair fromKeyValue(Slice slice, byte[] bytes)
    {
      return new Pair(GPOUtils.deserializeInt(slice.buffer), GPOUtils.deserializeInt(bytes));
    }

    @Override
    public Object fromByteArray(Slice fragment)
    {
      return new Pair(GPOUtils.deserializeInt(fragment.buffer, new MutableInt(fragment.offset)),
                      GPOUtils.deserializeInt(fragment.buffer, new MutableInt(fragment.offset + 4)));
    }

    @Override
    public Slice toByteArray(Pair o)
    {
      byte[] pairBytes = new byte[8];
      GPOUtils.serializeInt(o.key, pairBytes, new MutableInt(0));
      GPOUtils.serializeInt(o.value, pairBytes, new MutableInt(4));

      return new Slice(pairBytes);
    }

    @Override
    public int getPartition(Pair o)
    {
      return 0;
    }
  }

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(HDHTTestOperator.class);
}
