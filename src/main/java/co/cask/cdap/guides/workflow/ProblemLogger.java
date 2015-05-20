package co.cask.cdap.guides.workflow;

import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a custom action in the Workflow that could be used to send email notifications.
 */
public class ProblemLogger extends AbstractWorkflowAction {

  private static final Logger LOG = LoggerFactory.getLogger(ProblemLogger.class);

  @Override
  public void run() {
    LOG.info("Send email notification here about bad records.");
  }
}
