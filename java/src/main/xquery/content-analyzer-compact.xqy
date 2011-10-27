(:

~:)
declare default function namespace "urn:local";

declare default element namespace "http://marklogic.com/content-analyzer"; 	


declare variable $ELEMENT_FREQUENCY            := map:map();
declare variable $ELEMENT_VALUES               := map:map();
declare variable $ELEMENT_CHILD_FREQUENCY      := map:map();
declare variable $ELEMENT_CHILD_DISTANCE       := map:map();
declare variable $ELEMENT_ATTRIBUTE_FREQUENCY  := map:map();
declare variable $ELEMENT_ATTRIBUTE_VALUES     := map:map();
declare variable $ATTRIBUTE_FREQUENCY          := map:map();
declare variable $ATTRIBUTE_VALUES             := map:map();
declare variable $VALUE_QNAME                  := map:map();
declare variable $PREFIX_NAMESPACE             := map:map();
declare variable $NAMESPACE_VALUES             := map:map();

declare variable $EMPTY_VALUE                  := "##EMPTY##";

declare variable $ELEMENT_CHILD_JOINER  := "~~~";
declare variable $ATTRIBUTE_JOINER := "@@@";
declare variable $ELEMENT_NS_JOINER := "###";


declare function audit($node as node())
{
   typeswitch($node)
     case document-node() return audit-document($node)       
     case element() return audit-element($node)
     default return ()
};
(:~
 :
 :
~:)
declare function audit-element($node as element())
{
   let $map-key := fn:concat(fn:namespace-uri($node),$ELEMENT_NS_JOINER,fn:local-name($node))
   let $ns := map:put($NAMESPACE_VALUES,fn:namespace-uri($node),fn:namespace-uri($node))
   let $_ :=
      (
         map:put($ELEMENT_FREQUENCY,$map-key,((map:get($ELEMENT_FREQUENCY,$map-key),0)[1] +  1)),
         if($node/attribute::*) then
           for $attr in $node/@*
           let $ea-key := fn:concat($map-key,$ATTRIBUTE_JOINER,fn:namespace-uri($attr),$ELEMENT_NS_JOINER,fn:local-name($attr))
           let $av-key := fn:concat($ATTRIBUTE_JOINER,fn:namespace-uri($attr),$ELEMENT_NS_JOINER,fn:local-name($attr))        
           return
             (
               map:put($ELEMENT_ATTRIBUTE_FREQUENCY,$ea-key,(map:get($ELEMENT_ATTRIBUTE_FREQUENCY,$ea-key),0)[1] + 1),
               map:put($ELEMENT_ATTRIBUTE_VALUES,$ea-key,(map:get($ELEMENT_ATTRIBUTE_VALUES,$ea-key),fn:data($attr))),
               map:put($ATTRIBUTE_FREQUENCY,$av-key,(map:get($ATTRIBUTE_FREQUENCY,$av-key),0)[1] + 1),
               map:put($ATTRIBUTE_VALUES,$av-key,(map:get($ATTRIBUTE_VALUES,$av-key),fn:data($attr)))
             )
         else (),
         if($node/child::element()) 
         then
            for $c at $pos in $node/child::element()
            let $child-key := fn:concat($map-key,$ELEMENT_CHILD_JOINER,fn:namespace-uri($c),$ELEMENT_NS_JOINER,fn:local-name($c))
            return
              (
                 map:put($ELEMENT_CHILD_FREQUENCY,$child-key,(map:get($ELEMENT_CHILD_FREQUENCY,$child-key),0)[1] + 1),
                 map:put($ELEMENT_CHILD_DISTANCE,$child-key,(map:get($ELEMENT_CHILD_DISTANCE,$child-key),$pos))
              )
         else (:Complex Relationships:)
            if($node/text() and fn:count($node/text()) le 1) 
            then map:put($ELEMENT_VALUES,$map-key,(map:get($ELEMENT_VALUES,$map-key),$node/fn:string(.)))
            else map:put($ELEMENT_VALUES,$map-key,(map:get($ELEMENT_VALUES,$map-key),$EMPTY_VALUE)) 
      )
   return
     for $n in $node/element()
     return
       audit($n)
      
};

declare function audit-document($node as document-node())
{
   let $_ := 
   	for $n in $node/element()
        return 
            audit($n)
   let $stats-map := map:map()
   let $_ :=
     (
        map:put($stats-map,"ec",0),
        map:put($stats-map,"eac",0),
        map:put($stats-map,"pcc",0)

     )
   (:Element Statistics:)  
   let $elements := 
      for $k in map:keys($ELEMENT_FREQUENCY)
      let $e := map:get($ELEMENT_FREQUENCY,$k)
      let $values-map := map:map()
      let $element-values := map:get($ELEMENT_VALUES,$k)
      let $_ :=
         for $ev in $element-values
         return map:put($values-map,$ev,(map:get($values-map,$ev),0)[1] + 1)
      let $namespace := fn:tokenize($k,$ELEMENT_NS_JOINER)[1]
      let $name      := fn:tokenize($k,$ELEMENT_NS_JOINER)[2]
      order by $k
      return 
      	<e>
      	 <ek>{xdmp:hash32($k)}</ek>
      	 <en>{$name}</en>
      	 <ens>{$namespace}</ens>
      	 <e-f>{$e,map:put($stats-map,"ec",map:get($stats-map,"ec") + $e)}</e-f>
      	 <e-dv>{map:count($values-map)}</e-dv>
      	 <evs>
      	 {for $x in map:keys($values-map)
      	  return 
      	    <ev>
      	       <evn>{$x}</evn>
      	       <ev-f>{map:get($values-map,$x)}</ev-f>
      	    </ev>
      	 }
      	 </evs>
      	</e>
   (:Element Child Stats:)
   let $parent-childs := 
     for $k in map:keys($ELEMENT_CHILD_FREQUENCY)
     let $frequency := map:get($ELEMENT_CHILD_FREQUENCY,$k)
     let $distance := map:get($ELEMENT_CHILD_DISTANCE,$k)
     let $min-distance := fn:min($distance)
     let $max-distance := fn:max($distance)
     let $parent := 
     	let $p := fn:tokenize(fn:tokenize($k,$ELEMENT_CHILD_JOINER)[1],$ELEMENT_NS_JOINER)
     	return 
     	   <parent-element>
     	    <pck>{xdmp:hash32($k)}</pck>
     	    <ek>{xdmp:hash32(fn:string-join($p,$ELEMENT_CHILD_JOINER))}</ek>
     	    <pen>{$p[2]}</pen>
     	    <pens>{$p[1]}</pens>
     	   </parent-element>/*
     let $child  := 
     	let $c := fn:tokenize(fn:tokenize($k,$ELEMENT_CHILD_JOINER)[2],$ELEMENT_NS_JOINER)
        return
         <child-element>
            <cek>{xdmp:hash32(fn:string-join($c,$ELEMENT_NS_JOINER))}</cek>
     	    <cen>{$c[2]}</cen>
     	    <cens>{$c[1]}</cens>         
         </child-element>/*
     order by $parent/self::pens,$parent/self::pen,$min-distance
     return
        <pce>
        {$parent,
         $child
        }
         <pce-f>{$frequency,map:put($stats-map,"pcc",map:get($stats-map,"pcc") + $frequency)}</pce-f>
         <pce-mn>{$min-distance}</pce-mn>
         <pce-mx>{$max-distance}</pce-mx>
        </pce>
   (:Calculate Element Attribute Statistics:)
   let $element-attributes :=
      for $k in map:keys($ELEMENT_ATTRIBUTE_FREQUENCY)
      let $tokens := fn:tokenize($k,$ATTRIBUTE_JOINER)
      let $attribute-values-map := map:map()
      let $_ := 
          for $av in map:get($ELEMENT_ATTRIBUTE_VALUES,$k)
          return
              map:put($attribute-values-map,$av,(map:get($ELEMENT_ATTRIBUTE_VALUES,$av),$av))
      let $elem :=
          let $t := $tokens[1]
          return
       	  <attribute-element>
       	    <ek>{xdmp:hash32($t)}</ek>
      	    <ean>{fn:tokenize($t,$ELEMENT_NS_JOINER)[2]}</ean>
      	    <eans>{fn:tokenize($t,$ELEMENT_NS_JOINER)[1]}</eans>
      	  </attribute-element>/*
      let $attribute := 
        let $t := $tokens[2]
        return
      	  <ea>
      	    <eak>{xdmp:hash32($t)}</eak>
      	    <ean>{fn:tokenize($t,$ELEMENT_NS_JOINER)[2]}</ean>
      	    <eans>{fn:tokenize($t,$ELEMENT_NS_JOINER)[1]}</eans>
      	    <ea-dv>{map:count($attribute-values-map)}</ea-dv>
      	    <eavs>
      	    {
      	      for $av in map:keys($attribute-values-map)
      	      return
      	      <eav>
      	       <eavn>{$av}</eavn>
      	       <eav-f>{map:get($attribute-values-map,$av)}</eav-f>
      	      </eav>
      	    }
      	    </eavs>
      	  </ea>/*
      return
         <ea>
         <eak>{xdmp:hash32($k)}</eak>
         {
         $elem,
         $attribute,
         <ea-f>{map:get($ELEMENT_ATTRIBUTE_FREQUENCY,$k)}</ea-f>
         }</ea>
      
   let $attribute-values := 
      for $k in map:keys($ATTRIBUTE_FREQUENCY)
      let $distinct-values-map := map:map()
      let $_ := 
      	for $dv in map:get($ATTRIBUTE_VALUES,$k)
      	return
      	   map:put($distinct-values-map,$dv,((map:get($distinct-values-map,$dv),0)[1] + 1))
      return
         <av>
           <avk>{xdmp:hash32($k)}</avk>
           <avn>{fn:tokenize($k,$ELEMENT_NS_JOINER)[2]}</avn>
           <avns>{fn:tokenize(fn:tokenize($k,$ELEMENT_NS_JOINER)[1],$ATTRIBUTE_JOINER)[2]}</avns>
           <av-f>{map:get($ATTRIBUTE_FREQUENCY,$k)}</av-f>
           <av-dv>{map:count($distinct-values-map)}</av-dv>
           <avvs>
           {for $av in map:keys($distinct-values-map)
            order by $av
            return 
              <avv>
                <avvn>{$av}</avvn>
                <avv-f>{map:get($distinct-values-map,$av)}</avv-f>
              </avv>
           }</avvs>
         </av>   	 
   return
   <document>  
    {
       if($node/node() instance of element())
       then 
       (
         <ren>{fn:local-name($node/node())}</ren>,
         <rens>{fn:namespace-uri($node/node())}</rens>,
         <dt>xml</dt>
       ) 
       else if($node/node() instance of binary()) 
            then <dt>binary</dt>
            else if($node/node() instance of text())
            then <dt>text</dt>
            else ()
    }
    <ds>{
    if($node/node() instance of binary()) 
    then fn:string-length(fn:string(fn:data($node))) idiv 2
    else if($node/node() instance of element()) 
         then fn:string-length(xdmp:quote($node/node()))
         else if ($node/node() instance of text())
              then fn:string-length(fn:data($node/node())) 
              else ()
    }</ds>
    <dmt>{xdmp:uri-content-type($node/xdmp:node-uri(.))}</dmt>
    <nss>{
      for $ns in map:keys($NAMESPACE_VALUES)
      return
       <ns>{$ns}</ns>
    }</nss>
    <es count="{map:get($stats-map,"ec")}">{$elements}</es>
    <eas count="{map:get($stats-map,"eac")}">{$element-attributes}</eas>
    <pcs count="{map:get($stats-map,"pcc")}">{$parent-childs}</pcs>
    <avs count="{()}">{$attribute-values}</avs>
   </document>
};


for $x in fn:doc()
return
 try {
  xdmp:eval('
    declare variable $uri as xs:string external;
    declare variable $node as element() external;
    xdmp:document-insert($uri,$node)
  ',
  (
      fn:QName("","uri"), fn:base-uri($x),
      fn:QName("","node"),audit($x)
  )
  ,<options xmlns="xdmp:eval">
    <database>{xdmp:database("ContentAnalyzer")}</database>
    <isolation>different-transaction</isolation>
  </options>
  )
  } catch($ex) {} 
