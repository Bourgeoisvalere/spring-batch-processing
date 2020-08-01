package com.valere.learn.springbatchprocessing.config;

import com.valere.learn.springbatchprocessing.listener.JobCompletionNotificationListener;
import com.valere.learn.springbatchprocessing.entities.Person;
import com.valere.learn.springbatchprocessing.processor.PersonItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;

/**
 * The type Batch configuration.
 */
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {


    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    /**
     * Reader flat file item reader.
     *
     * @return the flat file item reader
     */
    @Bean
    public FlatFileItemReader<Person> reader(){
        return new FlatFileItemReaderBuilder<Person>()
                .name("PersonItemReader")
                .resource(new ClassPathResource("simple-data.csv"))
                .delimited()
                .names(new String[]{"firtsName", "lastName"})
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>(){{
                    setTargetType(Person.class);
                }}).build();
    }

    /**
     * Processor person item processor.
     *
     * @return the person item processor
     */
    @Bean
    public PersonItemProcessor processor(){
        return new PersonItemProcessor();
    }

    /**
     * Writer jdbc batch item writer.
     *
     * @param dataSource the data source
     * @return the jdbc batch item writer
     */
    @Bean
    public JdbcBatchItemWriter writer(DataSource dataSource){
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
                .dataSource(dataSource)
                .build();
    }

    /**
     * Import user job job.
     *
     * @param listener the listener
     * @param step1    the step 1
     * @return the job
     */
    @Bean
    public Job importUserJob(JobCompletionNotificationListener listener, Step step1){
        return jobBuilderFactory
                .get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .end()
                .build();
    }


    /**
     * Step 1 step.
     *
     * @param writer the writer
     * @return the step
     */
    @Bean
    public Step step1(JdbcBatchItemWriter<Person> writer) {
        return stepBuilderFactory.get("step1")
                .<Person, Person> chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer)
                .build();
    }


}
