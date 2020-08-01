package com.valere.learn.springbatchprocessing.processor;

import com.valere.learn.springbatchprocessing.entities.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;


/**
 * The type Person item processor.
 */
public class PersonItemProcessor implements ItemProcessor<Person, Person> {

    private static final Logger log = LoggerFactory.getLogger(PersonItemProcessor.class);

    /**
     * @param person
     * @return
     * @throws Exception
     */
    @Override
    public Person process(final Person person) throws Exception {
        String firstNameToUpperCase = person.getFirstName().toUpperCase();
        String lasttNameToUpperCase = person.getLastName().toUpperCase();

        Person personTransformed = new Person(firstNameToUpperCase, lasttNameToUpperCase);
        log.info("Converting ("+person+") to ("+personTransformed+")");
        return personTransformed;
    }
}
