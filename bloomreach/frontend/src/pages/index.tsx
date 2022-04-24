import Sidebar from "../components/sidebar";

const Dashboard = () => {
  return (
    <div className='view'>
      <Sidebar />
      <div className='model'>
        <div className='model-info'>
          <span className='model-title'>Select a model</span>
          <div className='model-specs'>
            <div className='model-spec'>
              <span className='title'>RMSE</span>
              <span className='content'>-</span>
            </div>
            <div className='model-spec'>
              <span className='title'>FACTOR / REGULARIZATION / ITERATIONS</span>
              <span className='content'>- / - / -</span>
            </div>
            <div className='model-spec'>
              <span className='title'>Final Score</span>
              <span className='content'>-</span>
            </div>
          </div>
        </div>
        <div className='model-content'></div>
      </div>
    </div>
  );
};
export default Dashboard;
