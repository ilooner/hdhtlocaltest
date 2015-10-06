/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.hdhtlocalmode;

public class Pair
{
  public Pair()
  {
  }

  public Pair(int key, int value)
  {
    this.key = key;
    this.value = value;
  }

  public int key;
  public int value;

  @Override
  public String toString()
  {
    return "Pair{" + "key=" + key + ", value=" + value + '}';
  }
}
