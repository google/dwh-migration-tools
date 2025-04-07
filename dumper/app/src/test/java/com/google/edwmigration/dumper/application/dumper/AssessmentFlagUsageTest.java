package com.google.edwmigration.dumper.application.dumper;

import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableCollection;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentAssessment;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;

public class AssessmentFlagUsageTest {
  private static final Logger logger = LoggerFactory.getLogger(AssessmentFlagUsageTest.class);

  @Test
  public void assessmentFlagIsExplicitlySpecified() throws Exception {
    ImmutableCollection<Connector> connectors = ConnectorRepository.getInstance().getAllConnectors();
    for (Connector connector : connectors) {
      RespectsArgumentAssessment assessment = AnnotationUtils.findAnnotation(connector.getClass(),
          RespectsArgumentAssessment.class);
      if (assessment != null) {
        assertThrows(
            Exception.class,
            () -> connector.validate(new ConnectorArguments()));
      } else {
        logger.warn("Should --assessment flag be required for the connector: {} ?", connector.getName());
      }
    }
  }
}
