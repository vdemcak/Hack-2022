import { Link } from "react-router-dom";
import Logo from "/logo.svg";

const Sidebar = () => {
  return (
    <div className='sidebar'>
      <img src={Logo} alt='Logo' />

      <span className='separator'>Models</span>

      <nav>
        <Link to='/straightforward'>Straightforward</Link>
        <Link to='/als-implicit'>ALS-Implicit</Link>
        <Link to='/als-apache-spark'>ALS-Apache Spark</Link>
        <Link to='/bpr-implicit'>BPR - Implicit</Link>
      </nav>
    </div>
  );
};
export default Sidebar;
