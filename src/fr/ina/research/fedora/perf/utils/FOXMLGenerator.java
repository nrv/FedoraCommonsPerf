/*
 * Copyright 2012 Institut National de l'Audiovisuel.
 * 
 * This file is part of FedoraCommonsPerf.
 * 
 * FedoraCommonsPerf is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * FedoraCommonsPerf is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with FedoraCommonsPerf. If not, see <http://www.gnu.org/licenses/>.
 */

package fr.ina.research.fedora.perf.utils;

import java.io.File;
import java.text.DecimalFormat;

import fr.ina.research.fedora.perf.LaunchPerfTest;

/**
 * FOXMLGenerator
 * 
 * @author Nicolas HERVE - nherve@ina.fr
 */
public class FOXMLGenerator {
	private int internalId;
	private DecimalFormat formater;

	public FOXMLGenerator() {
		super();
		internalId = 0;
		formater = new DecimalFormat("00000000");
	}

	public String getNextFOXML() {
		internalId++;

		File fakeImageFile = new File("/this/is/a/fake/image_" + formater.format(internalId) + ".jpg");
		String label = "Image " + internalId;
		String description = "Fake image file '" + fakeImageFile.getName() + "' to test Fedora Commons multithread performances";

		return produceFOXML(fakeImageFile, internalId, label, description);
	}

	private static String getPid(int id) {
		return LaunchPerfTest.NAMESPACE + ":" + id;
	}

	private String produceFOXML(File f, int id, String label, String description) {
		String pid = getPid(id);

		StringBuilder sb = new StringBuilder();
		sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		sb.append("<foxml:digitalObject xmlns:foxml=\"info:fedora/fedora-system:def/foxml#\" VERSION=\"1.1\" PID=\"" + pid + "\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"info:fedora/fedora-system:def/foxml#http://www.fedora.info/definitions/1/0/foxml1-1.xsd\">\n");
		sb.append("\t<foxml:objectProperties>\n");
		sb.append("\t\t<foxml:property NAME=\"info:fedora/fedora-system:def/model#state\" VALUE=\"A\" />\n");
		sb.append("\t\t<foxml:property NAME=\"info:fedora/fedora-system:def/model#label\" VALUE=\"" + label + "\" />\n");
		sb.append("\t</foxml:objectProperties>\n");
		sb.append("\t<foxml:datastream ID=\"DC\" STATE=\"A\" CONTROL_GROUP=\"X\">\n");
		sb.append("\t\t<foxml:datastreamVersion FORMAT_URI=\"http://www.openarchives.org/OAI/2.0/oai_dc/\" ID=\"DC.0\" MIMETYPE=\"text/xml\" LABEL=\"Dublin Core Record for this object\">\n");
		sb.append("\t\t\t<foxml:xmlContent>\n");
		sb.append("\t\t\t\t<oai_dc:dc xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\n");
		sb.append("\t\t\t\t\t<dc:title>" + label + "</dc:title>\n");
		sb.append("\t\t\t\t\t<dc:creator>nherve</dc:creator>\n");
		sb.append("\t\t\t\t\t<dc:subject>" + description + "</dc:subject>\n");
		sb.append("\t\t\t\t\t<dc:description>" + description + "</dc:description>\n");
		sb.append("\t\t\t\t\t<dc:publisher>INA - DRE</dc:publisher>\n");
		sb.append("\t\t\t\t\t<dc:identifier>" + pid + "</dc:identifier>\n");
		sb.append("\t\t\t\t</oai_dc:dc>\n");
		sb.append("\t\t\t</foxml:xmlContent>\n");
		sb.append("\t\t</foxml:datastreamVersion>\n");
		sb.append("\t</foxml:datastream>\n");
		sb.append("\t<foxml:datastream CONTROL_GROUP=\"E\" ID=\"IMAGE\" STATE=\"A\">\n");
		sb.append("\t\t<foxml:datastreamVersion ID=\"IMAGE.0\" MIMETYPE=\"image/jpeg\" LABEL=\"" + label + "\">\n");
		sb.append("\t\t\t<foxml:contentLocation REF=\"file:" + f.getAbsolutePath() + "\" TYPE=\"URL\" />\n");
		sb.append("\t\t</foxml:datastreamVersion>\n");
		sb.append("\t</foxml:datastream>\n");
		sb.append("</foxml:digitalObject>\n");
		return sb.toString();
	}
}
