package co.cask.cdap.guides.workflow;

import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is dummy action in the Workflow used to send email notification.
 */
public class EmailNotifier extends AbstractWorkflowAction {

  private static final Logger LOG = LoggerFactory.getLogger(EmailNotifier.class);

  @Override
  public void run() {
    LOG.info("Send email notification here about bad records.");
  }
}
