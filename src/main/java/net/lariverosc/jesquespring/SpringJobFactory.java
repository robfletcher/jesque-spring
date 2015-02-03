package net.lariverosc.jesquespring;

import net.greghaines.jesque.Job;
import net.greghaines.jesque.worker.JobFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class SpringJobFactory implements JobFactory, ApplicationContextAware {

    private final Logger logger = LoggerFactory.getLogger(SpringJobFactory.class);

    private ApplicationContext applicationContext;

    @Override
    public Object materializeJob(Job job) throws Exception {
        Runnable runnableJob = null;
        if (applicationContext.containsBeanDefinition(job.getClassName())) {//Lookup by bean Id
            runnableJob = (Runnable) applicationContext.getBean(job.getClassName(), job.getArgs());
        } else {
            try {
                Class clazz = Class.forName(job.getClassName());//Lookup by Class type
                String[] beanNames = applicationContext.getBeanNamesForType(clazz, true, false);
                if (applicationContext.containsBeanDefinition(job.getClassName())) {
                    runnableJob = (Runnable) applicationContext.getBean(beanNames[0], job.getArgs());
                } else {
                    if (beanNames != null && beanNames.length == 1) {
                        runnableJob = (Runnable) applicationContext.getBean(beanNames[0], job.getArgs());
                    }
                }
            } catch (ClassNotFoundException cnfe) {
                logger.error("Not bean Id or class definition found {}", job.getClassName());
                throw new Exception("Not bean Id or class definition found " + job.getClassName());
            }
        }
        return runnableJob;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }
}
