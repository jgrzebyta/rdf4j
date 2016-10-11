/**
 * *****************************************************************************
 * Copyright (c) 2016 Eclipse RDF4J contributors, Aduna, and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 ******************************************************************************
 */
package org.eclipse.rdf4j.repository.sail;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.common.iteration.Iterations;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.helpers.RioFileTypeDetector;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All tests are against {@link SailRepositoryConnection#add} methods.
 * 
 * @author gihtub.com:jgrzebyta
 */
public class AddMethodTest {

	SailRepository repository;

	SailRepositoryConnection connection;

	Logger log = LoggerFactory.getLogger(getClass());

	Path resource;

	@Rule
	public TemporaryFolder lucenetf = new TemporaryFolder();

	@Before
	public void setUp()
		throws Exception
	{
		LuceneSail sail = new LuceneSail();
		sail.setBaseSail(new MemoryStore());
		sail.setParameter(LuceneSail.LUCENE_DIR_KEY, "true");
		sail.setParameter(LuceneSail.LUCENE_DIR_KEY, lucenetf.getRoot().getAbsolutePath());
		log.debug("Lucene store: {}", lucenetf.getRoot().getAbsolutePath());
		repository = new SailRepository(sail);
		repository.initialize();
		connection = repository.getConnection();

		resource = Paths.get(Thread.currentThread().getContextClassLoader().getResource("beet.rdf").toURI());

		log.debug("resource path: {}", resource.toUri());
		assert resource.toFile().exists();
		assert resource.toFile().canRead();

		RioFileTypeDetector det = new RioFileTypeDetector();
		log.debug("file type: {}", det.probeContentType(resource));
	}

	@Test
	public void addFile()
		throws Exception
	{
		File resources = resource.toFile();
		//connection.begin(IsolationLevels.READ_COMMITTED);
		connection.add(resources, resource.toUri().toString(), RDFFormat.RDFXML, new Resource[] {});
		connection.commit();

		// validate
		long counts = countStatements(connection);
		Assert.assertEquals(68l, counts);
	}

	@Test
	public void addInputStream()
		throws Exception
	{
		InputStream resources = new BufferedInputStream(new FileInputStream(resource.toFile()));
		//connection.begin(IsolationLevels.READ_COMMITTED);
		connection.add(resources, resource.toUri().toString(), RDFFormat.RDFXML, new Resource[] {});
		connection.commit();

		// validate
		long counts = countStatements(connection);
		Assert.assertEquals(68l, counts);
	}

	@Test
	public void addReader()
		throws Exception
	{
		Reader resources = new BufferedReader(new FileReader(resource.toFile()));
		//connection.begin(IsolationLevels.READ_COMMITTED);
		connection.add(resources, resource.toUri().toString(), RDFFormat.RDFXML, new Resource[] {});
		connection.commit();

		// validate
		long counts = countStatements(connection);
		Assert.assertEquals(68l, counts);
	}

	public long countStatements(RepositoryConnection con)
		throws Exception
	{
		try {
			RepositoryResult<Statement> sts = connection.getStatements(null, null, null, new Resource[] {});
			int size = Iterations.asList(sts).size();
			log.debug("triples number: {}", size);
			return size;
		}
		catch (Exception e) {
			connection.rollback();
			throw e;
		}
	}
}
