


// ************** Generate the tree diagram	 *****************
//var margin = {top: 20, right: 120, bottom: 20, left: 120};
////var width = 3000 - margin.right - margin.left;
////var height = 1000 - margin.top - margin.bottom;
//
//var global_node_id_count = 0;
//var transiton_duration = 750;
//var root;
//
//var nodeWidth = 300;
//var nodeHeight = 150; // Needs to be dynamic in case lots of flame data,

//var d3Tree = d3.layout.tree()
////    .size([height, width])
//    .separation(function(a, b) {return a.parent == b.parent ? 1.5 : 2;})
//	.nodeSize([nodeWidth * 2, nodeHeight * 2]);
//
//var diagonal = d3.svg.diagonal()
//  .target(function(d) { return {x: d.target.x, y: d.target.y - nodeHeight}; })
//	.projection(function(d) { return [d.x + nodeWidth/2 , d.y + nodeHeight]; });
//
//var container = document.getElementById('chart-container');
//var width = container.offsetWidth;
//var height = container.offsetHeight;

//var zoom = d3.behavior.zoom()
//    .scaleExtent([-10, 2])
//    .on("zoom", zoomed);

          //var svg = d3.select('div#chart-container').append("svg")

//	.attr("width", width + margin.right + margin.left)
//	.attr("height", height + margin.top + margin.bottom)

    // .attr("width", '100%')
    // .attr("height", '100%')
    // .attr('viewBox','0 0 '+Math.min(width, height)+' '+Math.min(width, height))
    // .attr('preserveAspectRatio','xMinYMin')
    // .call(zoom);

// This transparent rect captures pointer events so that zoom works no matter where the cursor is.
var rect = svg.append("rect")
    .attr("width", width)
    .attr("height", height)
    .style("fill", "none")
    .style("pointer-events", "all");


// var svg = svg
//     .append("g")
//     .append("g").attr("transform", "translate(" + (margin.left + width/2) + "," + margin.top + ")")
//     ;

root = treeData[0];
root.x0 = height / 2;
root.y0 = 0;

update_node(root);

d3.select(self.frameElement)
    .style("height", "1000px");

function update_node(source_node) {
  // TODO: why does an update include re-executing everything that defines the graph?
  var nodes = createNodes(d3Tree, source_node);
  createLinks(d3Tree, source_node, nodes);
}

function createLinks(d3Tree, source_node, nodes) {

    // Compute the new d3Tree layout.
    var links = d3Tree.links(nodes);

    var link = svg.selectAll("path.link")
	  .data(links, function(d) { return d.target.id; });

  // Enter any new links at the parent's previous position.
  link.enter().insert("path", "g")
	  .attr("class", "link")
	  .attr("d", function(d) {
		  var o = {x: source_node.x0, y: source_node.y0};
		  return diagonal({source: o, target: o});
	  });

  // Transition links to their new position.
  link.transition()
	  .duration(transiton_duration)
	  .attr("d", diagonal);

  // Transition exiting nodes to the parent's new position.
  link.exit().transition()
	  .duration(transiton_duration)
	  .attr("d", function(d) {
		var o = {x: source_node.x, y: source_node.y};
		return diagonal({source: o, target: o});
	  })
	  .remove();
}

function click_name(node_name) {
    let node = get_node_by_name(root, node_name);
    if (node) {
        toggle_collapse_expand_children(node);
    }
    else {
        log("Couldn't find node: " + node_name);
    }
}

function get_node_by_name(root_node, name) {
    if (root_node['name'] == name) {
        return root_node
    }
    let result = null;
    if (root_node.hasOwnProperty('children')) {
        root_node['children'].forEach(function(c) {
            let found_child = get_node_by_name(c, name);
            if (found_child) {
                result = found_child;
            }
        });
    }
    return result;
}

// Toggle children on click.
function toggle_collapse_expand_children(d) {
  if (d.children) {
	d._children = d.children;
	d.children = null;
	d.children_collapsed = true;
  } else {
	d.children = d._children;
	d._children = null;
	d.children_collapsed = false;
  }
  update_node(d);
}

// function createNodeTitleHtml(data) {
//     return '<div style="text-align: center;"><strong>'+data.name+"</strong></div>";
// }

// function createNodeHtml(data) {
//     // TODO: replace with templating.
//     var body_extra = '';
//     if (data.children_collapsed) {
//         body_extra = "" + data._children.length + " collapsed nodes";
//     }
//     return '<div style="'
//     +   'overflow: hidden; text-overflow: ellipsis;'
//     +   ''
//     + '">'
//     + createNodeTitleHtml(data)
//     +   "<div>This is the body <br>"
//     +         body_extra
//     + "</div>"
//
//     +   '<div style="position: absolute; bottom: 0;">'
//     +       '<button onclick="click_name(\''+ data['name'] +'\');">'
//     +        '<i class="material-icons small">add</i></button>This is the footer</div>'
//     + "</div>"
//     ;
// }

function createNodes(d3Tree, inputNode) {

  // TODO: why does this always need to operate on the root?
  var tree_nodes = d3Tree.nodes(root).reverse();

  // Keep all nodes of the same depth on the same horizontal line.
  // let horizontal_node_spacing = 250;
  // tree_nodes.forEach(function(d) { d.y = d.depth * horizontal_node_spacing; });

  // TODO: this is ugly, but remove the supplied node to force a re-render in cause it has changed. There has to
  //    be a better way.
  svg.selectAll('#nodeid-'+inputNode.id).remove();

  var graph_nodes = svg.selectAll("g.node")
	  .data(tree_nodes, function(d) { return d.id || (d.id = ++global_node_id_count); });

  // Enter any new nodes at the parent's previous position.
  // graph_nodes.enter().append("g")
	//   .attr("class", "node")
	//   .attr("id", function(d) { return 'nodeid-' + d.id})
	//   .attr("transform", function(d) { return "translate(" + inputNode.x0 + "," + inputNode.y0 + ")"; })
	//   .append("foreignObject")
  //         .attr("width", nodeWidth)
  //         .attr("height", nodeHeight)
  //         .append("xhtml:div")
  //           .style("font", "14px 'Helvetica Neue'")
  //           .style("background", function(d) { return d._children ? "lightsteelblue" : "lightgreen"; })
  //           .style("width", nodeWidth + 'px')
  //           .style("height", nodeHeight + 'px')
  //           .style("border" , '1px solid black')
  //           .html(createNodeHtml)
  //         ;

  // Transition nodes to their new position.
  var graphNodesTransition = graph_nodes.transition()
	  .duration(transiton_duration)
	  .attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });

  graphNodesTransition //.selectAll("foreignObject")
      .attr("height", nodeHeight)
      .attr("width", nodeWidth)
	  ;


  // Transition exiting nodes to the parent's new position.
  graph_nodes.exit().transition()
	  .duration(transiton_duration)
	  .attr("transform", function(d) { return "translate(" + (inputNode.x+nodeWidth/2) + "," + (inputNode.y+nodeHeight) + ")"; })
	  .remove()
	  .select("foreignObject")
      .attr("height", 1e-6)
      .attr("width", 1e-6) ;

   // Stash the old positions for transition.
  tree_nodes.forEach(function(d) {
	d.x0 = d.x;
	d.y0 = d.y;
  });

  return tree_nodes;
}

//function zoomed() {
//  let t1 = "translate(" + d3.event.translate + ")";
//  let s1 = "scale(" + d3.event.scale + ")";
//  let t2 = "translate(" + (margin.left + width/2) + "," + margin.top + ")";
//
//  svg.attr("transform", t1 + s1 + t2);
//}
