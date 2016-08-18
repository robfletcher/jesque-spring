package net.lariverosc.jesquespring.job;

import net.lariverosc.jesquespring.JesqueSpringTest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Alejandro Riveros Cruz <lariverosc@gmail.com>
 */
public class MockJob implements Runnable {
    
    private Logger logger = LoggerFactory.getLogger(MockJob.class);

	public static int JOB_COUNT = 0;

	@Override
	public void run() {
	    logger.info("Job executed.");
		JOB_COUNT++;
	}
}
