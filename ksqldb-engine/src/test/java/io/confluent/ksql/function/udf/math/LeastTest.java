package io.confluent.ksql.function.udf.math;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.math.BigDecimal;
import org.junit.Before;
import org.junit.Test;


public class LeastTest {

  private Least udf;

  @Before
  public void setUp() {
    udf = new Least();
  }



  /*
  JUNIT
  -shouldHandleWithoutTypeCoercion
  -shouldHandleAllNullColumns
  -shouldHandleSomeNullColumns

  QTT
  -shouldWorkIfImplicitlyCastedToLong
  -ToDouble
  -ToDecimal (check schemas here for decimal casts, incl. coercion to decimal from other types))
  -NoParameters
  -Numeric to string type


   */

  @Test
  public void shouldWorkWithoutImplicitCasting(){
    assertThat(udf.least(0, 1, -1, 2, -2), is(-2));
    assertThat(udf.least(0L, 1L, -1L, 2L, -2L), is(-2L));
    assertThat(udf.least(0D, .1, -.1, .2, -.2), is(-.2));
    assertThat(udf.least(new BigDecimal("0"), new BigDecimal(".1"),
        new BigDecimal("-.1"), new BigDecimal(".2"), new BigDecimal("-.2")), is(new BigDecimal("-.2")));
    assertThat(udf.least("apple", "banana", "aardvark"), is("aardvark"));

  }

  @Test
  public void shouldHandleAllNullColumns(){
    //1. exclusively null columns return null
    assertThat(udf.least((Integer) null, null, null), is(nullValue()));
    assertThat(udf.least((Double) null, null, null), is(nullValue()));
    assertThat(udf.least((Long) null, null, null), is(nullValue()));
    assertThat(udf.least((BigDecimal) null, null, null), is(nullValue()));
    assertThat(udf.least((String) null, null, null), is(nullValue()));
  }

  @Test
  public void shouldHandleSomeNullColumns(){

    assertThat(udf.least(null, 27, null, 39, -49, -11, 68, 32, null, 101), is(-49));
    assertThat(udf.least(null, null, 39d, -49.01, -11.98, 68.1, .32, null, 101d), is(-49.01)); //break line or go over?
    assertThat(udf.least(null, 272038202439L, null, 39L, -4923740932490L, -11L, 68L, 32L, null, 101L), is(-4923740932490L));

    //todo: Note: Is this appreciably clearer than the assertThat method for objects as we explicitly call equals?
    assert udf.least(null, new BigDecimal("27"), null, new BigDecimal("-49"),
        new BigDecimal("11"), null).equals(new BigDecimal("-49"));
    assert udf.least(null, "apple", null, "banana", "kumquat", "aardvark", null).equals("aardvark");
  }


}
