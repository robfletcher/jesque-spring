package net.lariverosc.jesquespring;

import java.util.Arrays;
import junit.framework.Assert;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.meta.dao.FailureDAO;
import net.greghaines.jesque.meta.dao.KeysDAO;
import net.greghaines.jesque.meta.dao.QueueInfoDAO;
import net.greghaines.jesque.meta.dao.WorkerInfoDAO;
import net.lariverosc.jesquespring.job.MockJob;
import net.lariverosc.jesquespring.job.MockJobFail;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 *
 * @author Alejandro Riveros Cruz <lariverosc@gmail.com>
 */
public class JesqueSpringTest {

	private Client jesqueClient;
	private JesqueWorker jesqueWorker;
	private FailureDAO failureDAO;
	private KeysDAO keysDAO;
	private QueueInfoDAO queueInfoDAO;
	private WorkerInfoDAO workerInfoDAO;
	private JedisPool jedisPool;

	/**
	 *
	 */
	@BeforeClass
	public void setUp() {
		ApplicationContext applicationContext = new ClassPathXmlApplicationContext("classpath:test-context.xml");
		jesqueClient = (Client) applicationContext.getBean("jesqueClient");
		jesqueWorker = (JesqueWorker) applicationContext.getBean("jesqueWorker");
		failureDAO = (FailureDAO) applicationContext.getBean("failureDAO");
		keysDAO = (KeysDAO) applicationContext.getBean("keysDAO");
		queueInfoDAO = (QueueInfoDAO) applicationContext.getBean("queueInfoDAO");
		workerInfoDAO = (WorkerInfoDAO) applicationContext.getBean("workerInfoDAO");
		jedisPool = (JedisPool) applicationContext.getBean("jedisPool");
	}

	/**
	 *
	 */
	@BeforeMethod
	public void cleanUpRedis() {
		jesqueWorker.getWorker().togglePause(false);
		Jedis jedis = jedisPool.getResource();
		jedis.flushDB();
		jedisPool.returnResource(jedis);
	}

	/**
	 *
	 */
	@Test
	public void shouldAddJob() {
		jesqueWorker.getWorker().togglePause(true);
		Assert.assertEquals(0, queueInfoDAO.getPendingCount());
		MockJob.JOB_COUNT = 0;
		Job job = new Job(MockJob.class.getName(), new Object[]{});
		for (int i = 1; i <= 10; i++) {
			jesqueClient.enqueue("JESQUE_QUEUE", job);
			Assert.assertEquals(i, queueInfoDAO.getPendingCount());
		}
		jesqueWorker.getWorker().togglePause(false);
		waitJob(5000);
	}

	/**
	 *
	 */
	@Test(timeOut = 5000)
	public void shouldProcessJobsByClass() {
		jesqueWorker.getWorker().togglePause(true);
		Assert.assertEquals(0, queueInfoDAO.getPendingCount());
		MockJob.JOB_COUNT = 0;
		Job job = new Job(MockJob.class.getName(), new Object[]{});
		for (int i = 1; i <= 10; i++) {
			jesqueClient.enqueue("JESQUE_QUEUE", job);
		}
		jesqueWorker.getWorker().togglePause(false);
		waitJob(3000);
		Assert.assertEquals(10, MockJob.JOB_COUNT);
	}

	@Test(timeOut = 5000)
	public void shouldProcessJobsByBeanId() {
		jesqueWorker.getWorker().togglePause(true);
		Assert.assertEquals(0, queueInfoDAO.getPendingCount());
		MockJob.JOB_COUNT = 0;
		Job job = new Job("mockJob", new Object[]{});
		for (int i = 1; i <= 10; i++) {
			jesqueClient.enqueue("JESQUE_QUEUE", job);
		}
		jesqueWorker.getWorker().togglePause(false);
		waitJob(3000);
		Assert.assertEquals(10, MockJob.JOB_COUNT);
	}

	/**
	 *
	 */
	@Test
	public void shouldAddJobWithArguments() {
		jesqueWorker.getWorker().togglePause(true);
		Object[] args = new Object[]{1, 2.3, true, "test", Arrays.asList("inner", 4.5)};
		Job job = new Job(MockJob.class.getName(), args);
		jesqueClient.enqueue("JESQUE_QUEUE", job);
		jesqueWorker.getWorker().togglePause(false);

	}

	/**
	 *
	 */
	@Test
	public void shouldAddJobWithEmptyArguments() {
		jesqueWorker.getWorker().togglePause(true);
		Job job = new Job(MockJob.class.getName(), new Object[]{});
		jesqueClient.enqueue("JESQUE_QUEUE", job);
		jesqueWorker.getWorker().togglePause(false);

	}

	/**
	 *
	 */
	@Test
	public void shouldRegisterFailJob() {
		Job job = new Job(MockJobFail.class.getName(), new Object[]{});
		jesqueClient.enqueue("JESQUE_QUEUE", job);
		waitJob(3000);
		Assert.assertEquals(1, failureDAO.getCount());
	}

	/**
	 *
	 * @param milliseconds
	 */
	public void waitJob(long milliseconds) {
		try {
			Thread.sleep(milliseconds);
		} catch (InterruptedException ex) {
		}
	}
}
