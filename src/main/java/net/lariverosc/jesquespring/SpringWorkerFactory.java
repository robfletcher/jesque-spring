package net.lariverosc.jesquespring;

import java.util.Collection;
import java.util.concurrent.Callable;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerEvent;
import net.greghaines.jesque.worker.WorkerImpl;
import net.greghaines.jesque.worker.WorkerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 *
 * @author Alejandro Riveros Cruz <lariverosc@gmail.com>
 */
public class SpringWorkerFactory implements Callable<WorkerImpl>, ApplicationContextAware {

	private Logger logger = LoggerFactory.getLogger(SpringWorkerFactory.class);
	private final Config config;
	private final Collection<String> queues;
	private ApplicationContext applicationContext;

	/**
	 * Create a new factory. Returned
	 * <code>WorkerImpl</code>s will use the provided arguments.
	 *
	 * @param config used to create a connection to Redis
	 * @param queues the list of queues to poll
	 * @param jobTypes the list of job types to execute
	 */
	public SpringWorkerFactory(final Config config, final Collection<String> queues) {
		this.config = config;
		this.queues = queues;
	}


	/**
	 * Create a new
	 * <code>SpringWorkerImpl</code> using the arguments provided to this factory's constructor.
	 */
	@Override
	public WorkerImpl call() {
		WorkerImpl springWorker = new SpringWorker(this.config, this.queues);
		((SpringWorker) springWorker).setApplicationContext(this.applicationContext);
		springWorker.addListener(new WorkerListener() {
			@Override
			public void onEvent(WorkerEvent event, Worker worker, String queue, net.greghaines.jesque.Job job, Object runner, Object result, Exception ex) {
				logger.debug("event {}, worker {}, queue {}", new Object[]{event.name(), worker.getName(), queue});
				switch (event) {
					case JOB_EXECUTE:
						break;
					case JOB_FAILURE:
						break;
					case JOB_PROCESS:
						break;
					case JOB_SUCCESS:
						break;
					case WORKER_ERROR:
						break;
					case WORKER_POLL:
						break;
					case WORKER_START:
						break;
					case WORKER_STOP:
						break;
				}
			}
		});
		return springWorker;
	}

	

	/**
	 *
	 * @param applicationContext
	 * @throws BeansException
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
}