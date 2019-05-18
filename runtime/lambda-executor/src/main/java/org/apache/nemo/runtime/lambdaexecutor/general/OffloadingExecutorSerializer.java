package org.apache.nemo.runtime.lambdaexecutor.general;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;
import org.apache.nemo.compiler.frontend.beam.transform.coders.GBKFinalStateCoder;
import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.offloading.common.OffloadingEncoder;
import org.apache.nemo.offloading.common.OffloadingSerializer;
import org.apache.nemo.runtime.lambdaexecutor.middle.MiddleOffloadingOutputDecoder;
import org.apache.nemo.runtime.lambdaexecutor.middle.MiddleOffloadingOutputEncoder;

public class OffloadingExecutorSerializer implements OffloadingSerializer {

  private final OffloadingDecoder inputDecoder;
  private final OffloadingEncoder outputEncoder;
  private final OffloadingDecoder outputDecoder;

  public OffloadingExecutorSerializer(final Coder<UnboundedSource.CheckpointMark> coder,
                                      final Coder<GBKFinalState> stateCoder) {
    this.inputDecoder = new OffloadingExecutorInputDecoder(coder, stateCoder);
    this.outputEncoder = new MiddleOffloadingOutputEncoder(coder, stateCoder);
    this.outputDecoder = new MiddleOffloadingOutputDecoder(coder, stateCoder);
  }

  @Override
  public OffloadingEncoder getInputEncoder() {
    return null;
  }

  @Override
  public OffloadingDecoder getInputDecoder() {
    return inputDecoder;
  }

  @Override
  public OffloadingEncoder getOutputEncoder() {
    return outputEncoder;
  }

  @Override
  public OffloadingDecoder getOutputDecoder() {
    return outputDecoder;
  }


}
