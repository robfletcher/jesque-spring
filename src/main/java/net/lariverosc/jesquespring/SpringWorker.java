/*
 * Copyright 2012 Alejandro Riveros Cruz <lariverosc@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.lariverosc.jesquespring;

import java.util.Collection;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.worker.WorkerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 *
 * @author Alejandro Riveros Cruz <lariverosc@gmail.com>
 */
public class SpringWorker extends WorkerImpl implements ApplicationContextAware {

    private Logger logger = LoggerFactory.getLogger(SpringWorker.class);

    private ApplicationContext applicationContext;

    /**
     *
     * @param config used to create a connection to Redis
     * @param queues the list of queues to poll
     */
    public SpringWorker(final Config config, final Collection<String> queues, String workerGroupName) {
        super(config, queues, new SpringJobFactory(), workerGroupName);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        ((SpringJobFactory)getJobFactory()).setApplicationContext(applicationContext);
    }

    /**
     * Convenient initialization method for the Spring container
     */
    public void init() {
        logger.info("Start a new thread for SpringWorker");
        new Thread(this).start();
    }

    /**
     * Convenient destroy method for the Spring container
     */
    public void destroy() {
        logger.info("End the SpringWorker thread");
        end(true);
    }
}
