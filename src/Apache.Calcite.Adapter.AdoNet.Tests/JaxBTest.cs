using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using java.io;
using java.lang;
using java.nio.charset;
using java.util;

using javax.xml.bind;
using javax.xml.bind.annotation;
using javax.xml.parsers;
using javax.xml.transform;
using javax.xml.transform.dom;
using javax.xml.transform.stream;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.eclipse.persistence.dynamic;
using org.eclipse.persistence.jaxb;
using org.eclipse.persistence.jaxb.dynamic;
using org.w3c.dom;


namespace Apache.Calcite.Adapter.AdoNet.Tests
{


    [XmlAccessorType(XmlAccessType.__Enum.FIELD)]
    public class Address
    {

        public string street;

        public string city;

    }

    [XmlRootElement]
    [XmlAccessorType(XmlAccessType.__Enum.FIELD)]
    public class Customer
    {

        public string name;

        public Address address;

    }

    [TestClass]
    public class JaxBTest
    {

        static string XML = """
        <?xml version="1.0" encoding="UTF-8"?>
        <customer>
           <name>Jane Doe</name>
           <address>
              <!-- comment -->
              <street>2 NEW STREET</street>
              <city>Any Town</city>
           </address>
           <phone-number type="home">555-HOME</phone-number>
           <phone-number type="cell">555-CELL</phone-number>
           <phone-number type="work">555-WORK</phone-number>
        </customer>
        """;

        [TestMethod]
        public void Main()
        {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);
            // Preserve Lexical Info
            dbf.setCoalescing(false);
            dbf.setExpandEntityReferences(false);
            dbf.setIgnoringElementContentWhitespace(false);
            dbf.setIgnoringComments(false);
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document document = db.parse(new ByteArrayInputStream(java.lang.String.instancehelper_getBytes(XML, StandardCharsets.UTF_8)));

            var props = new HashMap();
            //props.put("javax.xml.bind.context.factory", "org.eclipse.persistence.jaxb.dynamic.DynamicJAXBContextFactory");
            DynamicJAXBContextFactory.createContext([], props);
            var jc = (DynamicJAXBContext)org.eclipse.persistence.jaxb.JAXBContext.newInstance("cli.Apache.Calcite.Adapter.Ado.Tests", Thread.currentThread().getContextClassLoader(), props);
            var binder = jc.createBinder();
            DynamicEntity customer = (DynamicEntity)binder.unmarshal(document);
            customer.set("foo", "bar");
            binder.updateXML(customer);

            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer t = tf.newTransformer();
            var @out = new ByteArrayOutputStream();
            t.transform(new DOMSource(document), new StreamResult(@out));
            var str = @out.toString();


        }

    }
}
